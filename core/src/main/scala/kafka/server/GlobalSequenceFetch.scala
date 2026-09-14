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

import org.apache.kafka.common.errors.{CorruptRecordException, InvalidRequestException, RecordTooLargeException}
import org.apache.kafka.common.message.{FetchGlobalSequenceRequestData, FetchGlobalSequenceResponseData, ReadGlobalSequenceDataRequestData, ReadGlobalSequenceDataResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.{AbstractResponse, FetchGlobalSequenceResponse, ReadGlobalSequenceDataRequest, ReadGlobalSequenceDataResponse}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup

/** Wire contract: complete original batches, with a separate global selection and continuation. */
object GlobalSequenceFetch {
  val MaxBatchBytes: Int = 8 * 1024 * 1024
  val MaxResponseBytes: Int = 16 * 1024 * 1024
  // More than the maximum flexible response header and 1000 mapping entries (without records).
  val MaxPayloadBytes: Int = MaxResponseBytes - 128 * 1024
  val ParallelReads: Int = 4

  case class Request(lookup: GlobalSequenceLookup.Request, maxBytes: Int) {
    require(maxBytes > 0 && maxBytes <= MaxPayloadBytes, s"maxBytes must be between 1 and $MaxPayloadBytes")
  }

  case class Data(records: MemoryRecords, sourceLeaderEpoch: Int, dataHighWatermark: Long)

  def request(data: FetchGlobalSequenceRequestData): Request = {
    if (data.isolationLevel() != 0)
      throw new InvalidRequestException("FetchGlobalSequence v0 supports only READ_UNCOMMITTED (0)")
    Request(new GlobalSequenceLookup.Request(data.topicId(), data.globalStartOffset(), data.globalEndOffsetExclusive(),
      data.maxBatches(), data.timeoutMs()), data.maxBytes())
  }

  def physical(data: ReadGlobalSequenceDataRequestData): PhysicalBatch = {
    require(data.sourceLeaderEpoch() >= 0, "sourceLeaderEpoch must be non-negative")
    require(data.timeoutMs() > 0 && data.timeoutMs() <= GlobalSequenceLookup.MAX_TIMEOUT_MS, "Invalid physical read timeout")
    new PhysicalBatch(new PartitionKey(data.topicId(), data.partition()), data.physicalBaseOffset(),
      data.physicalLastOffset(), data.recordCount())
  }

  def dataRequest(batch: PhysicalBatch, epoch: Int, timeoutMs: Int): ReadGlobalSequenceDataRequest.Builder =
    new ReadGlobalSequenceDataRequest.Builder(new ReadGlobalSequenceDataRequestData().setTopicId(batch.partition().topicId())
      .setPartition(batch.partition().partition()).setPhysicalBaseOffset(batch.baseOffset()).setPhysicalLastOffset(batch.lastOffset())
      .setRecordCount(batch.recordCount()).setSourceLeaderEpoch(epoch).setTimeoutMs(timeoutMs))

  def dataResponse(batch: PhysicalBatch, data: Data): ReadGlobalSequenceDataResponse =
    new ReadGlobalSequenceDataResponse(new ReadGlobalSequenceDataResponseData().setTopicId(batch.partition().topicId())
      .setPartition(batch.partition().partition()).setSourceLeaderEpoch(data.sourceLeaderEpoch)
      .setDataHighWatermark(data.dataHighWatermark).setRecords(data.records))

  def decode(batch: PhysicalBatch, epoch: Int, response: AbstractResponse): Data = response match {
    case reply: ReadGlobalSequenceDataResponse =>
      val data = reply.data()
      val error = Errors.forCode(data.errorCode())
      if (error != Errors.NONE) throw error.exception()
      if (data.topicId() != batch.partition().topicId() || data.partition() != batch.partition().partition() ||
        data.sourceLeaderEpoch() != epoch || data.dataHighWatermark() <= batch.lastOffset())
        throw new InvalidRequestException("Physical fetch response does not match its fenced request or HW")
      val records = data.records() match {
        case memory: MemoryRecords => memory
        case _ => throw new CorruptRecordException("Physical fetch must return in-memory records")
      }
      validateBatch(batch, records)
      Data(records, epoch, data.dataHighWatermark())
    case _ => throw new InvalidRequestException("Unexpected global sequence data response")
  }

  /** Does not decompress records on network callbacks. The source worker validates their offsets. */
  def validateBatch(expected: PhysicalBatch, records: MemoryRecords): Unit = {
    if (records.sizeInBytes() > MaxBatchBytes) throw new RecordTooLargeException("Global fetch batch exceeds 8 MiB")
    val batches = records.batches().iterator()
    if (!batches.hasNext) throw new CorruptRecordException("Missing complete physical batch")
    val batch = batches.next()
    batch.ensureValid()
    if (batch.magic() != RecordBatch.MAGIC_VALUE_V2 || batch.isControlBatch ||
      batch.baseOffset() != expected.baseOffset() || batch.lastOffset() != expected.lastOffset() ||
      batch.countOrNull() == null || batch.countOrNull().intValue() != expected.recordCount() ||
      batch.sizeInBytes() != records.sizeInBytes() || batches.hasNext)
      throw new CorruptRecordException("Physical records differ from the committed global mapping")
  }

  def response(request: Request, snapshot: GlobalSequenceLookup.Snapshot): FetchGlobalSequenceResponseData =
    new FetchGlobalSequenceResponseData().setTopicId(request.lookup.topicId()).setIndexTopicId(snapshot.indexTopicId())
      .setIndexPartition(snapshot.indexPartition()).setCoordinatorLeaderEpoch(snapshot.leaderEpoch())
      .setIndexHighWatermark(snapshot.indexHighWatermark()).setCommittedGlobalEndOffset(snapshot.committedGlobalEnd())
      .setNextGlobalOffset(request.lookup.startOffset())

  def fetched(request: Request, mapping: GlobalSequenceLookup.Mapping, data: Data): FetchGlobalSequenceResponseData.FetchedBatch = {
    val batch = mapping.batch()
    new FetchGlobalSequenceResponseData.FetchedBatch().setGlobalBaseOffset(mapping.globalBaseOffset())
      .setPhysicalPartition(batch.partition().partition()).setPhysicalBaseOffset(batch.baseOffset()).setPhysicalLastOffset(batch.lastOffset())
      .setRecordCount(batch.recordCount()).setSelectedGlobalStartOffset(math.max(request.lookup.startOffset(), mapping.globalBaseOffset()))
      .setSelectedGlobalEndOffset(math.min(request.lookup.endOffset(), mapping.globalEndOffset())).setRecords(data.records)
  }

  def fail(data: FetchGlobalSequenceResponseData, error: Throwable): FetchGlobalSequenceResponse = {
    val cause = Errors.maybeUnwrapException(error)
    val code = if (cause.isInstanceOf[IllegalArgumentException]) Errors.INVALID_REQUEST else Errors.forException(cause)
    new FetchGlobalSequenceResponse(data.setErrorCode(code.code()).setErrorMessage(code.message()))
  }
}
