/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.{TopicIdPartition, TopicPartition}
import org.apache.kafka.common.errors.{CorruptRecordException, CoordinatorNotAvailableException, FencedLeaderEpochException, KafkaStorageException, NotCoordinatorException, NotLeaderOrFollowerException, RecordTooLargeException, ThrottlingQuotaExceededException, TimeoutException}
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, RecordBatch, Records}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.{BufferSupplier, KafkaThread, ThreadUtils, Time}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinatorRecordSerde, GlobalSequenceLookup}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup.{Mapping, Request, Result, Snapshot}
import org.apache.kafka.coordinator.globalsequence.generated.{BatchIndexKey, BatchIndexValue, IndexerFenceKey, TopicMetadataKey, TopicMetadataValue}
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.LogReadInfo

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.{ArrayBlockingQueue, CompletableFuture, ConcurrentHashMap, ExecutorService, RejectedExecutionException, ScheduledFuture, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[server] object GlobalSequenceIndexReader {
  val ReadBytes = 1024 * 1024
  val MaxBatchBytes = 8 * ReadBytes
  def workers(threads: Int = 2, queueSize: Int = 128): ExecutorService = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](queueSize), runnable => KafkaThread.daemon("global-sequence-lookup", runnable),
    new ThreadPoolExecutor.AbortPolicy)
}

/** Cold historical lookup. No unbounded mapping cache and no disk I/O on coordinator event threads. */
class GlobalSequenceIndexReader private[server](replicas: ReplicaManager, scheduler: Scheduler, time: Time,
                                                workers: ExecutorService = GlobalSequenceIndexReader.workers(),
                                                resources: GlobalSequenceResources = null)
  extends GlobalSequenceLookup.Reader {
  import GlobalSequenceIndexReader._

  private val pending = ConcurrentHashMap.newKeySet[CompletableFuture[Result]]()
  @volatile private var closed = false

  override def read(request: Request, snapshot: Snapshot, deadlineNs: Long): CompletableFuture[Result] = {
    val result = new CompletableFuture[Result]()
    val lease = try Option(resources).map(_.acquire(Scope.INDEX_READ, request.topicId(), GlobalSequenceResources.BATCH_BYTES))
    catch { case NonFatal(error) => return CompletableFuture.failedFuture(error) }
    @volatile var completionError: Throwable = null
    val state = new AtomicInteger(0) // queued, running, released
    def releaseQueued(): Unit = if (state.compareAndSet(0, 2)) lease.foreach(_.finish(completionError))
    @volatile var timer: ScheduledFuture[_] = null
    val work: Runnable = () => {
      if (state.compareAndSet(0, 1)) try {
        if (!result.isDone) try result.complete(scan(request, snapshot, () => {
          if (closed || result.isDone || time.nanoseconds() >= deadlineNs)
            throw new TimeoutException("Global index lookup was cancelled or expired")
        })) catch {
          case error @ (_: NotLeaderOrFollowerException | _: FencedLeaderEpochException) =>
            result.completeExceptionally(new NotCoordinatorException("Index leader changed during lookup", error))
          case NonFatal(error) => result.completeExceptionally(error)
        }
      } finally {
        state.set(2)
        lease.foreach(_.finish(completionError))
      }
    }
    pending.add(result)
    result.whenComplete { (_, error) =>
      completionError = error
      releaseQueued()
      pending.remove(result)
      if (timer != null) timer.cancel(false)
      workers match {
        case pool: ThreadPoolExecutor => pool.remove(work)
        case _ =>
      }
    }
    if (closed) result.completeExceptionally(new CoordinatorNotAvailableException("Global index reader is closed"))
    else try {
      val remaining = math.max(0L, deadlineNs - time.nanoseconds())
      timer = scheduler.scheduleOnce("global-sequence-lookup-timeout", () =>
        result.completeExceptionally(new TimeoutException("Global index lookup deadline expired")),
        (remaining + 999999L) / 1000000L)
      if (result.isDone) timer.cancel(false)
      else workers.execute(work)
      if (closed) result.completeExceptionally(new CoordinatorNotAvailableException("Global index reader is closed"))
    } catch {
      case _: RejectedExecutionException => result.completeExceptionally(new ThrottlingQuotaExceededException(100, "Global lookup queue is full"))
      case NonFatal(error) => result.completeExceptionally(error)
    }
    result
  }

  private def scan(request: Request, snapshot: Snapshot, checkDeadline: () => Unit): Result = {
    checkDeadline()
    val tp = new TopicIdPartition(snapshot.indexTopicId(), new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, snapshot.indexPartition()))
    val source = replicas.getPartitionOrException(tp)
    val epoch = Optional.of(Int.box(snapshot.leaderEpoch()))
    val log = source.localLogWithEpochOrThrow(epoch, requireLeader = true)
    def fetch(offset: Long, bytes: Int): LogReadInfo = {
      val params = new FetchParams(FetchRequest.CONSUMER_REPLICA_ID, -1L, 0L, 1, bytes, FetchIsolation.LOG_END, Optional.empty())
      val data = new FetchRequest.PartitionData(tp.topicId(), offset, FetchRequest.INVALID_LOG_START_OFFSET,
        bytes, epoch, Optional.empty())
      source.fetchRecords(params, data, time.milliseconds(), bytes, minOneMessage = bytes > 0, updateFetchState = false)
    }
    def validate(): Unit = {
      checkDeadline()
      if ((replicas.getPartitionOrException(tp) ne source) || (source.localLogWithEpochOrThrow(epoch, requireLeader = true) ne log))
        throw new NotCoordinatorException("Index partition or log was replaced during lookup")
      if (log.topicId().isEmpty || log.topicId().get() != snapshot.indexTopicId())
        throw new NotCoordinatorException("Index log UUID changed during lookup")
      val bounds = fetch(log.logEndOffset(), 0)
      if (bounds.logStartOffset != 0 || log.localLogStartOffset() != 0)
        throw new KafkaStorageException("Global index history is missing its prefix")
      if (bounds.highWatermark < snapshot.indexHighWatermark() || bounds.logEndOffset < snapshot.indexHighWatermark())
        throw new NotCoordinatorException("Index log no longer covers the captured committed boundary")
      if (source.localLogWithEpochOrThrow(epoch, requireLeader = true) ne log)
        throw new NotCoordinatorException("Index log changed while validating lookup")
    }
    try {
      validate()
      val end = math.min(request.endOffset(), snapshot.committedGlobalEnd())
      val selected = mutable.ArrayBuffer.empty[Mapping]
      val serde = new GlobalSequenceCoordinatorRecordSerde
      var scannedBytes = 0L
      val scanLimit = Option(resources).map(_.config().lookupScanMaxBytes().toLong).getOrElse(Long.MaxValue)
      var cursor = 0L
      var globalCursor = 0L
      var staged: Option[Mapping] = None
      var stagedOffset = -1L
      var done = request.startOffset() == end
      val buffers = BufferSupplier.create()
      try {
        while (!done && cursor < snapshot.indexHighWatermark()) {
          validate()
          val info = fetch(cursor, ReadBytes)
          if (info.divergingEpoch.isPresent) throw new NotCoordinatorException("Index log diverged during lookup")
          scannedBytes += info.fetchedData.records.sizeInBytes()
          if (scannedBytes > scanLimit)
            throw new ThrottlingQuotaExceededException(100, "Global lookup scan byte budget exhausted")
          val records = copyRecords(info.fetchedData.records)
          val batches = records.batches().iterator()
          val before = cursor
          while (!done && cursor < snapshot.indexHighWatermark() && batches.hasNext) {
            val batch = batches.next()
            batch.ensureValid()
            if (batch.magic() != RecordBatch.MAGIC_VALUE_V2 || batch.isControlBatch || batch.isTransactional || batch.baseOffset() != cursor)
              throw new CorruptRecordException("Invalid global index batch or missing index offsets")
            val iterator = batch.streamingIterator(buffers)
            try {
              while (!done && cursor < snapshot.indexHighWatermark() && iterator.hasNext) {
                checkDeadline()
                val record = iterator.next()
                if (record.offset() != cursor) throw new CorruptRecordException("Non-contiguous global index records")
                val decoded = serde.deserialize(record.key(), record.value())
                if (decoded.value() == null) throw new CorruptRecordException("Global index tombstone is not supported")
                decoded.key() match {
                  case key: BatchIndexKey =>
                    if (staged.nonEmpty) throw new CorruptRecordException("Incomplete global allocation pair")
                    val value = decoded.value().message().asInstanceOf[BatchIndexValue]
                    staged = Some(new Mapping(key.globalBaseOffset(), new PhysicalBatch(new PartitionKey(key.topicId(), value.physicalPartition()),
                      value.physicalBaseOffset(), value.physicalLastOffset(), value.recordCount())))
                    stagedOffset = cursor
                  case key: TopicMetadataKey =>
                    val value = decoded.value().message().asInstanceOf[TopicMetadataValue]
                    val mapping = staged.getOrElse(throw new CorruptRecordException("Missing BatchIndex before TopicMetadata"))
                    if (stagedOffset + 1 != cursor || key.topicId() != mapping.batch().partition().topicId() || value.nextGlobalOffset() != mapping.globalEndOffset())
                      throw new CorruptRecordException("Mismatched global allocation pair")
                    if (key.topicId() == request.topicId()) {
                      if (mapping.globalBaseOffset() != globalCursor || mapping.globalEndOffset() > snapshot.committedGlobalEnd())
                        throw new CorruptRecordException("Global index range contradicts the committed snapshot")
                      globalCursor = mapping.globalEndOffset()
                      if (mapping.globalEndOffset() > request.startOffset() && mapping.globalBaseOffset() < end) selected += mapping
                      done = globalCursor >= end || selected.size >= request.maxBatches()
                    }
                    staged = None
                  case _: IndexerFenceKey =>
                    if (staged.nonEmpty) throw new CorruptRecordException("Fence interrupts a global allocation pair")
                  case _ => throw new CorruptRecordException("Unknown global index record")
                }
                cursor += 1
              }
            } finally iterator.close()
            if (!done && cursor < snapshot.indexHighWatermark() && cursor != batch.lastOffset() + 1)
              throw new CorruptRecordException("Global index batch record count does not match its offsets")
          }
          if (cursor == before) throw new CorruptRecordException("Missing global index records below the captured HW")
        }
        if (!done || staged.nonEmpty) throw new CorruptRecordException("Committed global range is missing its allocation records")
      } finally buffers.close()
      validate()
      new Result(snapshot, selected.toList.asJava,
        if (selected.isEmpty) request.startOffset() else math.min(end, selected.last.globalEndOffset()))
    } catch {
      case NonFatal(error) =>
        // Prefer fencing over apparent corruption caused by a concurrent log replacement/truncation.
        validate()
        error match {
          case _: NotLeaderOrFollowerException | _: FencedLeaderEpochException => throw new NotCoordinatorException("Index leader changed", error)
          case io: java.io.IOException => throw new KafkaStorageException("Failed to read global index history", io)
          case _ => throw error
        }
    }
  }

  private def copyRecords(records: Records): MemoryRecords = {
    if (records.sizeInBytes() > MaxBatchBytes) throw new RecordTooLargeException("Index batch exceeds the 8 MiB lookup read limit")
    records match {
      case memory: MemoryRecords => MemoryRecords.readableRecords(memory.buffer().duplicate())
      case file: FileRecords =>
        val buffer = ByteBuffer.allocate(file.sizeInBytes())
        file.readInto(buffer, 0)
        if (buffer.remaining() != file.sizeInBytes()) throw new CorruptRecordException("Short read of global index history")
        MemoryRecords.readableRecords(buffer)
      case _ => throw new CorruptRecordException("Unsupported global index records")
    }
  }

  override def close(): Unit = {
    closed = true
    pending.asScala.foreach(_.completeExceptionally(new CoordinatorNotAvailableException("Global index reader is closed")))
    workers.shutdownNow()
    ThreadUtils.shutdownExecutorServiceQuietly(workers, 5, TimeUnit.SECONDS)
  }
}
