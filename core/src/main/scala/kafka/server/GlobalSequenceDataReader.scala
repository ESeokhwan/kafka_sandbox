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

import org.apache.kafka.common.{TopicIdPartition, TopicPartition}
import org.apache.kafka.common.errors.{CorruptRecordException, CoordinatorNotAvailableException, FencedLeaderEpochException, InvalidRequestException, KafkaStorageException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException, RecordTooLargeException, ThrottlingQuotaExceededException, TimeoutException, UnknownTopicIdException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, Records}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.{BufferSupplier, KafkaThread, ThreadUtils, Time}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.LogReadInfo

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.{ArrayBlockingQueue, CompletableFuture, ConcurrentHashMap, ExecutorService, RejectedExecutionException, ScheduledFuture, ThreadPoolExecutor, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[server] object GlobalSequenceDataReader {
  def workers(): ExecutorService = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](128), runnable => KafkaThread.daemon("global-sequence-data-read", runnable),
    new ThreadPoolExecutor.AbortPolicy)
}

/** Copies one complete mapped batch below raw data HW, fenced by topic UUID and leader epoch. */
class GlobalSequenceDataReader private[server](replicas: ReplicaManager, scheduler: Scheduler, time: Time,
                                               workers: ExecutorService = GlobalSequenceDataReader.workers()) extends AutoCloseable {
  import GlobalSequenceFetch._

  private val pending = ConcurrentHashMap.newKeySet[CompletableFuture[Data]]()
  @volatile private var closed = false

  def read(batch: PhysicalBatch, epoch: Int, deadlineNs: Long): CompletableFuture[Data] = {
    val result = new CompletableFuture[Data]()
    @volatile var timer: ScheduledFuture[_] = null
    val work: Runnable = () => {
      if (!result.isDone) try result.complete(readBatch(batch, epoch, () => {
        if (closed || result.isDone || time.nanoseconds() >= deadlineNs)
          throw new TimeoutException("Global data fetch was cancelled or expired")
      })) catch {
        case error @ (_: NotLeaderOrFollowerException | _: FencedLeaderEpochException) =>
          result.completeExceptionally(new NotLeaderOrFollowerException("Data leader changed during fetch", error))
        case NonFatal(error) => result.completeExceptionally(error)
      }
    }
    pending.add(result)
    result.whenComplete { (_, _) =>
      pending.remove(result)
      if (timer != null) timer.cancel(false)
      workers match {
        case pool: ThreadPoolExecutor => pool.remove(work)
        case _ =>
      }
    }
    if (closed) result.completeExceptionally(new CoordinatorNotAvailableException("Global data reader is closed"))
    else try {
      val remaining = math.max(0L, deadlineNs - time.nanoseconds())
      timer = scheduler.scheduleOnce("global-sequence-data-timeout", () =>
        result.completeExceptionally(new TimeoutException("Global data fetch deadline expired")),
        (remaining + 999999L) / 1000000L)
      if (result.isDone) timer.cancel(false)
      else workers.execute(work)
      if (closed) result.completeExceptionally(new CoordinatorNotAvailableException("Global data reader is closed"))
    } catch {
      case _: RejectedExecutionException => result.completeExceptionally(new ThrottlingQuotaExceededException(100, "Global data read queue is full"))
      case NonFatal(error) => result.completeExceptionally(error)
    }
    result
  }

  private def readBatch(expected: PhysicalBatch, leaderEpoch: Int, deadline: () => Unit): Data = {
    deadline()
    require(leaderEpoch >= 0, "sourceLeaderEpoch must be non-negative")
    val key = expected.partition()
    val name = replicas.metadataCache.getTopicName(key.topicId())
    if (name.isEmpty) throw new UnknownTopicIdException("Unknown global sequence source UUID")
    val tp = new TopicIdPartition(key.topicId(), new TopicPartition(name.get(), key.partition()))
    val source = replicas.getPartitionOrException(tp)
    val epoch = Optional.of(Int.box(leaderEpoch))
    val log = source.localLogWithEpochOrThrow(epoch, requireLeader = true)
    def fetch(offset: Long, bytes: Int): LogReadInfo = {
      val params = new FetchParams(FetchRequest.CONSUMER_REPLICA_ID, -1L, 0L, 1, bytes, FetchIsolation.LOG_END, Optional.empty())
      val request = new FetchRequest.PartitionData(key.topicId(), offset, FetchRequest.INVALID_LOG_START_OFFSET,
        bytes, epoch, Optional.empty())
      source.fetchRecords(params, request, time.milliseconds(), bytes, minOneMessage = bytes > 0, updateFetchState = false)
    }
    def validate(): Long = {
      deadline()
      if ((replicas.getPartitionOrException(tp) ne source) || (source.localLogWithEpochOrThrow(epoch, requireLeader = true) ne log))
        throw new NotLeaderOrFollowerException("Source partition or log changed during global fetch")
      if (log.topicId().isEmpty || log.topicId().get() != key.topicId())
        throw new UnknownTopicIdException("Source log UUID differs from the mapped topic")
      if (Topic.isInternal(name.get()) || !log.config().globalSequenceEnabled() || log.config().compact)
        throw new InvalidRequestException("Global fetch requires a global sequence data topic without compaction")
      // Read raw HW without rounding an offset inside a batch down to its base.
      val offsets = fetch(log.logEndOffset(), 0)
      if (source.localLogWithEpochOrThrow(epoch, requireLeader = true) ne log)
        throw new NotLeaderOrFollowerException("Source log changed while capturing offsets")
      if (expected.baseOffset() < math.max(offsets.logStartOffset, log.localLogStartOffset()))
        throw new OffsetOutOfRangeException("The mapped physical batch is no longer available in the local source log")
      if (expected.lastOffset() >= offsets.highWatermark || expected.lastOffset() >= offsets.logEndOffset)
        throw new OffsetNotAvailableException("The current source leader has not exposed the entire mapped batch below HW")
      offsets.highWatermark
    }
    val highWatermark = validate()
    try {
      val info = fetch(expected.baseOffset(), 1)
      if (info.divergingEpoch.isPresent) throw new NotLeaderOrFollowerException("Source log diverged during global fetch")
      if (info.fetchedData.firstEntryIncomplete) throw new CorruptRecordException("Incomplete first physical batch")
      val original = copy(info.fetchedData.records)
      validateBatch(expected, original)
      val buffers = BufferSupplier.create()
      try {
        val iterator = original.batches().iterator().next().skipKeyValueIterator(buffers)
        var offset = expected.baseOffset()
        try {
          while (iterator.hasNext) {
            deadline()
            if (iterator.next().offset() != offset || offset > expected.lastOffset())
              throw new CorruptRecordException("Mapped source records contain missing or out-of-order offsets")
            offset += 1
          }
          if (offset != expected.lastOffset() + 1) throw new CorruptRecordException("Mapped source record count is inconsistent")
        } finally iterator.close()
      } finally buffers.close()
      if (validate() < highWatermark) throw new NotLeaderOrFollowerException("Source HW regressed during global fetch")
      Data(original, leaderEpoch, highWatermark)
    } catch {
      case NonFatal(error) =>
        validate()
        error match {
          case io: java.io.IOException => throw new KafkaStorageException("Failed to read mapped global sequence data", io)
          case _ => throw error
        }
    }
  }

  private def copy(records: Records): MemoryRecords = {
    if (records.sizeInBytes() > MaxBatchBytes) throw new RecordTooLargeException("Global fetch batch exceeds 8 MiB")
    // A one-byte minOneMessage fetch returns exactly one complete batch. Never retain or close the live FileRecords slice.
    val buffer = ByteBuffer.allocate(records.sizeInBytes())
    records match {
      case memory: MemoryRecords => buffer.put(memory.buffer().duplicate()); buffer.flip()
      case file: FileRecords => file.readInto(buffer, 0)
      case _ => throw new CorruptRecordException("Unsupported mapped source records")
    }
    if (buffer.remaining() != records.sizeInBytes()) throw new CorruptRecordException("Short read of mapped source batch")
    MemoryRecords.readableRecords(buffer)
  }

  override def close(): Unit = {
    closed = true
    pending.asScala.foreach(_.completeExceptionally(new CoordinatorNotAvailableException("Global data reader is closed")))
    workers.shutdownNow()
    ThreadUtils.shutdownExecutorServiceQuietly(workers, 5, TimeUnit.SECONDS)
  }
}
