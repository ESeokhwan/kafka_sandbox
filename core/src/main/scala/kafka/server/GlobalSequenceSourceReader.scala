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

import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition}
import org.apache.kafka.common.errors.{CorruptRecordException, InvalidRequestException, KafkaStorageException, NotLeaderOrFollowerException, OffsetOutOfRangeException, UnknownTopicIdException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, MutableRecordBatch, RecordBatch, Records}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.{BufferSupplier, Time}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.storage.internals.log.{LogReadInfo, UnifiedLog}

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Optional
import scala.util.control.NonFatal

object GlobalSequenceSourceReader {
  /** BATCH requires an index commit before consuming nextPhysicalOffset; CONTINUE skips only control batches. */
  sealed trait ReadStatus
  case object BATCH extends ReadStatus
  case object CONTINUE extends ReadStatus
  case object AWAIT_HIGH_WATERMARK extends ReadStatus

  case class ReadResult(
    partition: PartitionKey,
    sourceLeaderEpoch: Int,
    dataHighWatermark: Long,
    nextPhysicalOffset: Long,
    batch: Option[PhysicalBatch],
    status: ReadStatus
  )

  /** Recovery must recheck authoritative index progress before deciding whether this gap is unrecoverable. */
  class SourceLogGapException(
    val partition: PartitionKey,
    val requestedOffset: Long,
    val logStartOffset: Long,
    val localLogStartOffset: Long,
    val logEndOffset: Long,
    reason: String
  ) extends KafkaException(s"Global sequence source gap for $partition at $requestedOffset " +
    s"(logStart=$logStartOffset, localLogStart=$localLogStartOffset, logEnd=$logEndOffset): $reason")
}

/**
 * Synchronous, bounded local-log reader for the partition indexer. Call on its worker executor, never
 * from a HW/leadership callback or while holding a partition lock. No payload or log handle escapes a read.
 * The caller owns its cursor and must recheck its indexer generation before applying asynchronous results.
 */
class GlobalSequenceSourceReader(replicaManager: ReplicaManager, time: Time = Time.SYSTEM) {
  import GlobalSequenceSourceReader._

  def read(partition: PartitionKey, sourceLeaderEpoch: Int, physicalOffset: Long, maxBytes: Int): ReadResult = {
    require(partition != null, "partition must be supplied")
    require(sourceLeaderEpoch >= 0, "sourceLeaderEpoch must be non-negative")
    require(physicalOffset >= 0, "physicalOffset must be non-negative")
    require(maxBytes > 0, "maxBytes must be positive")
    val topic = replicaManager.metadataCache.getTopicName(partition.topicId())
    if (topic.isEmpty) throw new UnknownTopicIdException(s"Unknown source topic ${partition.topicId()}")
    val topicIdPartition = new TopicIdPartition(partition.topicId(), new TopicPartition(topic.get(), partition.partition()))
    val localPartition = replicaManager.getPartitionOrException(topicIdPartition)
    val epoch = Optional.of(Int.box(sourceLeaderEpoch))
    val log = localPartition.localLogWithEpochOrThrow(epoch, requireLeader = true)
    validateLog(topicIdPartition, log)
    def fetch(offset: Long, limit: Int): LogReadInfo = {
      val params = new FetchParams(FetchRequest.CONSUMER_REPLICA_ID, -1L, 0L, 1, limit,
        FetchIsolation.LOG_END, Optional.empty())
      val data = new FetchRequest.PartitionData(partition.topicId(), offset, FetchRequest.INVALID_LOG_START_OFFSET,
        limit, epoch, Optional.empty())
      localPartition.fetchRecords(params, data, time.milliseconds(), limit, minOneMessage = limit > 0, updateFetchState = false)
    }

    def captureOffsets(): LogReadInfo = {
      // fetchOffsetSnapshot materializes offset metadata and may round an HW inside a batch down
      // to that batch's base. A zero-byte LOG_END fetch captures raw offsets without changing HW.
      // Seeking to LEO avoids traversing the payload; zero-byte fetches never request a full first batch.
      try fetch(log.logEndOffset(), 0)
      catch {
        case error: OffsetOutOfRangeException =>
          throw new NotLeaderOrFollowerException("Source log changed while capturing its offsets", error)
      }
    }

    val snapshot = captureOffsets()
    val highWatermark = snapshot.highWatermark

    def gap(offset: Long, current: LogReadInfo, reason: String): Nothing =
      throw new SourceLogGapException(partition, offset, current.logStartOffset, log.localLogStartOffset(),
        current.logEndOffset, reason)

    def checkBounds(current: LogReadInfo): Unit = {
      if (physicalOffset < math.max(current.logStartOffset, log.localLogStartOffset()) || physicalOffset > current.logEndOffset)
        gap(physicalOffset, current, "The resume position is outside the available local log; do not skip to log start")
    }

    def revalidate(): Unit = {
      val currentPartition = replicaManager.getPartitionOrException(topicIdPartition)
      if (currentPartition ne localPartition) throw new NotLeaderOrFollowerException("Source partition was replaced while reading")
      if (localPartition.localLogWithEpochOrThrow(epoch, requireLeader = true) ne log)
        throw new NotLeaderOrFollowerException("Source log was replaced while reading")
      // The fetch takes the partition lock and fences leader changes, including A -> B -> A.
      val current = captureOffsets()
      if (localPartition.localLogWithEpochOrThrow(epoch, requireLeader = true) ne log)
        throw new NotLeaderOrFollowerException("Source log was replaced while capturing offsets")
      validateLog(topicIdPartition, log)
      checkBounds(current)
      if (current.highWatermark < highWatermark)
        gap(physicalOffset, current, "Source high watermark regressed within the captured leader epoch")
    }

    def result(offset: Long, batch: Option[PhysicalBatch], status: ReadStatus): ReadResult =
      ReadResult(partition, sourceLeaderEpoch, highWatermark, offset, batch, status)

    try {
      checkBounds(snapshot)
      val readResult = if (physicalOffset >= highWatermark) {
        result(physicalOffset, None, AWAIT_HIGH_WATERMARK)
      } else {
        // Read up to LOG_END, then enforce the captured HW below. An HW in the middle of a batch
        // can make a HIGH_WATERMARK fetch empty; that is a wait, not evidence of a missing batch.
        val info = fetch(physicalOffset, maxBytes)
        if (info.divergingEpoch.isPresent) throw new NotLeaderOrFollowerException("Source log diverged while reading")
        if (info.fetchedData.firstEntryIncomplete)
          throw new CorruptRecordException("Source read did not return a complete first batch despite minOneMessage")
        if (info.fetchedData.records.sizeInBytes() == 0)
          gap(physicalOffset, snapshot, "No local batch exists below the captured source high watermark")
        val records = copyRecords(info.fetchedData.records)
        scan(records, partition, sourceLeaderEpoch, physicalOffset, highWatermark,
          offset => gap(offset, snapshot, "The next physical batch starts after the required resume position"))
      }
      revalidate()
      readResult
    } catch {
      case NonFatal(error) =>
        // A concurrent leadership/log change can also cause a short file read or apparent corruption.
        // Surface the invalidated source first, so recovery does not diagnose the old log as current.
        revalidate()
        error match {
          case _: OffsetOutOfRangeException => gap(physicalOffset, snapshot, "The local log no longer contains the resume position")
          case io: IOException => throw new KafkaStorageException("Failed to read the global sequence source log", io)
          case _ => throw error
        }
    }
  }

  private def validateLog(partition: TopicIdPartition, log: UnifiedLog): Unit = {
    if (log.topicId().isEmpty || log.topicId().get() != partition.topicId())
      throw new UnknownTopicIdException("Source log topic ID does not match the requested topic")
    if (Topic.isInternal(partition.topic()) || !log.config().globalSequenceEnabled() || log.config().compact)
      throw new InvalidRequestException("Source reader requires a global sequence data topic without compaction")
  }

  private def copyRecords(records: Records): MemoryRecords = records match {
    case memory: MemoryRecords => MemoryRecords.readableRecords(memory.buffer().duplicate())
    case file: FileRecords =>
      // The fetch size is bounded by maxBytes, except that one complete oversized first batch is allowed.
      // FileRecords is a borrowed slice of the live log. Never close it or expose it to the indexer.
      val buffer = ByteBuffer.allocate(file.sizeInBytes())
      file.readInto(buffer, 0)
      if (buffer.remaining() != file.sizeInBytes()) throw new CorruptRecordException("Short read of the source log slice")
      MemoryRecords.readableRecords(buffer)
    case _ => throw new InvalidRequestException("Unsupported source log records implementation")
  }

  private def scan(
    records: MemoryRecords,
    partition: PartitionKey,
    epoch: Int,
    start: Long,
    highWatermark: Long,
    gap: Long => Nothing
  ): ReadResult = {
    var cursor = start
    val batches = records.batches().iterator()
    val buffers = BufferSupplier.create()
    try {
      if (!batches.hasNext) throw new CorruptRecordException("Incomplete first batch in the source log")
      while (cursor < highWatermark && batches.hasNext) {
        val batch = batches.next()
        if (batch.magic() != RecordBatch.MAGIC_VALUE_V2)
          throw new UnsupportedForMessageFormatException("Global sequence source batches must use record format v2")
        batch.ensureValid()
        if (batch.baseOffset() > cursor) gap(cursor)
        if (batch.baseOffset() < cursor)
          throw new InvalidRequestException(s"Source cursor $cursor is not a physical batch boundary (${batch.baseOffset()})")
        val count = batch.countOrNull()
        if (count == null || count <= 0 || batch.baseOffset() < 0 || batch.lastOffset() < batch.baseOffset() ||
          batch.lastOffset() == Long.MaxValue || batch.lastOffset() - batch.baseOffset() != count.longValue() - 1)
          throw new CorruptRecordException("Source batch bounds and record count are inconsistent")
        if (batch.lastOffset() >= highWatermark)
          return ReadResult(partition, epoch, highWatermark, cursor, None, AWAIT_HIGH_WATERMARK)
        validateRecords(batch, buffers)
        cursor = batch.lastOffset() + 1
        if (!batch.isControlBatch) {
          val physicalBatch = new PhysicalBatch(partition, batch.baseOffset(), batch.lastOffset(), count)
          return ReadResult(partition, epoch, highWatermark, cursor, Some(physicalBatch), BATCH)
        }
      }
      // A slice may end in a partial trailing batch or at a segment boundary. Resume at the last
      // fully validated control batch; minOneMessage will return the next complete batch on reread.
      ReadResult(partition, epoch, highWatermark, cursor, None,
        if (cursor >= highWatermark) AWAIT_HIGH_WATERMARK else CONTINUE)
    } finally buffers.close()
  }

  private def validateRecords(batch: MutableRecordBatch, buffers: BufferSupplier): Unit = {
    val iterator = batch.skipKeyValueIterator(buffers)
    var expectedOffset = batch.baseOffset()
    try {
      while (iterator.hasNext) {
        val record = iterator.next()
        if (record.offset() != expectedOffset || expectedOffset > batch.lastOffset())
          throw new CorruptRecordException("Source batch contains missing or out-of-order physical records")
        expectedOffset += 1
      }
      if (expectedOffset != batch.lastOffset() + 1)
        throw new CorruptRecordException("Source batch record count does not match its physical range")
    } finally iterator.close()
  }
}
