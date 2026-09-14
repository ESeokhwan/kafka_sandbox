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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.{LookupGlobalSequenceResponseData, ReadGlobalSequenceIndexRequestData, ReadGlobalSequenceIndexResponseData}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup
import scala.jdk.CollectionConverters._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.{AppendGlobalSequenceIndexRequestData, DescribeGlobalSequencePartitionRequestData, RegisterGlobalSequenceIndexerRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard._

import java.util.{Optional, OptionalLong}

/** Wire conversion for the router. Error responses never supply usable ownership or progress. */
private[server] object GlobalSequenceProtocol {
  def lookupRequest(request: GlobalSequenceLookup.Request, epoch: Int): ReadGlobalSequenceIndexRequest.Builder =
    new ReadGlobalSequenceIndexRequest.Builder(new ReadGlobalSequenceIndexRequestData().setTopicId(request.topicId())
      .setGlobalStartOffset(request.startOffset()).setGlobalEndOffsetExclusive(request.endOffset())
      .setMaxBatches(request.maxBatches()).setTimeoutMs(request.timeoutMs()).setCoordinatorLeaderEpoch(epoch))

  def lookupResponse(request: GlobalSequenceLookup.Request, response: AbstractResponse): GlobalSequenceLookup.Result = response match {
    case reply: ReadGlobalSequenceIndexResponse =>
      val data = reply.data()
      val error = Errors.forCode(data.errorCode())
      if (error != Errors.NONE) throw error.exception()
      if (data.topicId() != request.topicId()) throw new InvalidRequestException("Lookup response identifies a different topic")
      val snapshot = new GlobalSequenceLookup.Snapshot(data.indexTopicId(), data.indexPartition(), data.coordinatorLeaderEpoch(),
        data.indexHighWatermark(), data.committedGlobalEndOffset())
      val end = math.min(request.endOffset(), snapshot.committedGlobalEnd())
      if (request.startOffset() > end || data.batches().size() > request.maxBatches())
        throw new InvalidRequestException("Lookup response violates the range or page size")
      var next = request.startOffset()
      val mappings = data.batches().asScala.zipWithIndex.map { case (batch, position) =>
        val mapping = new GlobalSequenceLookup.Mapping(batch.globalBaseOffset(), new PhysicalBatch(
          new PartitionKey(request.topicId(), batch.physicalPartition()), batch.physicalBaseOffset(), batch.physicalLastOffset(), batch.recordCount()))
        if (next >= end || mapping.globalBaseOffset() > next || mapping.globalEndOffset() <= next ||
          (position > 0 && mapping.globalBaseOffset() != next) || mapping.globalEndOffset() > snapshot.committedGlobalEnd() ||
          batch.selectedGlobalStartOffset() != next || batch.selectedGlobalEndOffset() != math.min(end, mapping.globalEndOffset()))
          throw new InvalidRequestException("Lookup mappings are not a contiguous selection of the requested range")
        next = batch.selectedGlobalEndOffset()
        mapping
      }
      if (next != data.nextGlobalOffset() || (next < end && mappings.size < request.maxBatches()))
        throw new InvalidRequestException("Lookup response has an inconsistent continuation offset")
      new GlobalSequenceLookup.Result(snapshot, mappings.toList.asJava, next)
    case _ => throw new InvalidRequestException("Unexpected global sequence lookup response type")
  }

  def publicLookupResponse(request: GlobalSequenceLookup.Request, result: GlobalSequenceLookup.Result): LookupGlobalSequenceResponse = {
    val snapshot = result.snapshot()
    val data = new LookupGlobalSequenceResponseData().setTopicId(request.topicId()).setIndexTopicId(snapshot.indexTopicId())
      .setIndexPartition(snapshot.indexPartition()).setCoordinatorLeaderEpoch(snapshot.leaderEpoch())
      .setIndexHighWatermark(snapshot.indexHighWatermark()).setCommittedGlobalEndOffset(snapshot.committedGlobalEnd())
      .setNextGlobalOffset(result.nextGlobalOffset())
    result.mappings().forEach { mapping =>
      val batch = mapping.batch()
      data.batches().add(new LookupGlobalSequenceResponseData.BatchMapping().setGlobalBaseOffset(mapping.globalBaseOffset())
        .setPhysicalPartition(batch.partition().partition()).setPhysicalBaseOffset(batch.baseOffset()).setPhysicalLastOffset(batch.lastOffset())
        .setRecordCount(batch.recordCount()).setSelectedGlobalStartOffset(math.max(request.startOffset(), mapping.globalBaseOffset()))
        .setSelectedGlobalEndOffset(math.min(request.endOffset(), mapping.globalEndOffset())))
    }
    new LookupGlobalSequenceResponse(data)
  }

  def internalLookupResponse(request: GlobalSequenceLookup.Request, result: GlobalSequenceLookup.Result): ReadGlobalSequenceIndexResponse = {
    val snapshot = result.snapshot()
    val data = new ReadGlobalSequenceIndexResponseData().setTopicId(request.topicId()).setIndexTopicId(snapshot.indexTopicId())
      .setIndexPartition(snapshot.indexPartition()).setCoordinatorLeaderEpoch(snapshot.leaderEpoch())
      .setIndexHighWatermark(snapshot.indexHighWatermark()).setCommittedGlobalEndOffset(snapshot.committedGlobalEnd())
      .setNextGlobalOffset(result.nextGlobalOffset())
    result.mappings().forEach { mapping =>
      val batch = mapping.batch()
      data.batches().add(new ReadGlobalSequenceIndexResponseData.BatchMapping().setGlobalBaseOffset(mapping.globalBaseOffset())
        .setPhysicalPartition(batch.partition().partition()).setPhysicalBaseOffset(batch.baseOffset()).setPhysicalLastOffset(batch.lastOffset())
        .setRecordCount(batch.recordCount()).setSelectedGlobalStartOffset(math.max(request.startOffset(), mapping.globalBaseOffset()))
        .setSelectedGlobalEndOffset(math.min(request.endOffset(), mapping.globalEndOffset())))
    }
    new ReadGlobalSequenceIndexResponse(data)
  }

  def describeRequest(partition: PartitionKey): DescribeGlobalSequencePartitionRequest.Builder =
    new DescribeGlobalSequencePartitionRequest.Builder(new DescribeGlobalSequencePartitionRequestData()
      .setTopicId(partition.topicId()).setPartition(partition.partition()))

  def registerRequest(request: RegistrationRequest, epoch: Int): RegisterGlobalSequenceIndexerRequest.Builder =
    new RegisterGlobalSequenceIndexerRequest.Builder(new RegisterGlobalSequenceIndexerRequestData()
      .setTopicId(request.partition().topicId()).setPartition(request.partition().partition())
      .setSourceBrokerId(request.sourceBrokerId()).setSourceLeaderEpoch(request.sourceLeaderEpoch())
      .setExpectedGeneration(request.expectedGeneration()).setRegistrationId(request.registrationId()).setCoordinatorLeaderEpoch(epoch))

  def appendRequest(request: AppendRequest, epoch: Int): AppendGlobalSequenceIndexRequest.Builder = {
    val batch = request.batch()
    val owner = request.indexer()
    new AppendGlobalSequenceIndexRequest.Builder(new AppendGlobalSequenceIndexRequestData()
      .setTopicId(batch.partition().topicId()).setPartition(batch.partition().partition())
      .setPhysicalBaseOffset(batch.baseOffset()).setPhysicalLastOffset(batch.lastOffset()).setRecordCount(batch.recordCount())
      .setPredecessorBaseOffset(request.predecessorBaseOffset()).setDataHighWatermark(request.dataHighWatermark())
      .setSourceBrokerId(owner.brokerId()).setSourceLeaderEpoch(owner.leaderEpoch()).setIndexerGeneration(owner.generation())
      .setRegistrationId(owner.registrationId()).setCoordinatorLeaderEpoch(epoch))
  }

  private def check(partition: PartitionKey, topicId: Uuid, physicalPartition: Int, errorCode: Short): Unit = {
    val error = Errors.forCode(errorCode)
    if (error != Errors.NONE) throw error.exception()
    if (partition.topicId() != topicId || partition.partition() != physicalPartition)
      throw new InvalidRequestException("Global sequence response identifies a different data partition")
  }

  private def progress(partition: PartitionKey, base: Long, last: Long, count: Int): Optional[PhysicalBatch] = {
    if (base == -1 && last == -1 && count == 0) Optional.empty()
    else Optional.of(new PhysicalBatch(partition, base, last, count))
  }

  private def indexer(broker: Int, epoch: Int, generation: Long, id: Uuid): Optional[IndexerIdentity] = {
    if (broker == -1 && epoch == -1 && generation == -1 && id == Uuid.ZERO_UUID) Optional.empty()
    else Optional.of(new IndexerIdentity(broker, epoch, generation, id))
  }

  def describeResponse(partition: PartitionKey, response: AbstractResponse): PartitionDescription = response match {
    case reply: DescribeGlobalSequencePartitionResponse =>
      val data = reply.data()
      check(partition, data.topicId(), data.partition(), data.errorCode())
      new PartitionDescription(progress(partition, data.physicalBaseOffset(), data.physicalLastOffset(), data.recordCount()),
        indexer(data.sourceBrokerId(), data.sourceLeaderEpoch(), data.indexerGeneration(), data.registrationId()))
    case _ => throw new InvalidRequestException("Unexpected global sequence describe response type")
  }

  def registerResponse(request: RegistrationRequest, response: AbstractResponse): RegistrationResponse = response match {
    case reply: RegisterGlobalSequenceIndexerResponse =>
      val data = reply.data()
      check(request.partition(), data.topicId(), data.partition(), data.errorCode())
      val owner = indexer(data.sourceBrokerId(), data.sourceLeaderEpoch(), data.indexerGeneration(), data.registrationId())
      if (data.registered() && (owner.isEmpty || owner.get() != new IndexerIdentity(request.sourceBrokerId(),
        request.sourceLeaderEpoch(), request.expectedGeneration() + 1, request.registrationId())))
        throw new InvalidRequestException("Global sequence registration response identifies a different owner")
      new RegistrationResponse(data.registered(), owner, data.coordinatorLeaderEpoch())
    case _ => throw new InvalidRequestException("Unexpected global sequence registration response type")
  }

  def appendResponse(request: AppendRequest, response: AbstractResponse): AppendResponse = response match {
    case reply: AppendGlobalSequenceIndexResponse =>
      val data = reply.data()
      val partition = request.batch().partition()
      check(partition, data.topicId(), data.partition(), data.errorCode())
      val batch = new PhysicalBatch(partition, data.physicalBaseOffset(), data.physicalLastOffset(), data.recordCount())
      if (batch != request.batch()) throw new InvalidRequestException("Global sequence append response identifies a different physical batch")
      val status = AppendStatus.forCode(data.status())
      val through = progress(partition, data.indexedThroughBaseOffset(), data.indexedThroughLastOffset(), data.indexedThroughRecordCount())
      if ((status == AppendStatus.INDEXED || status == AppendStatus.ALREADY_INDEXED) &&
        (through.isEmpty || through.get().lastOffset() < batch.lastOffset()))
        throw new InvalidRequestException("Successful global sequence append response does not cover the requested batch")
      if (status == AppendStatus.INDEXED) {
        if (data.globalBaseOffset() < 0 || data.globalBaseOffset() > Long.MaxValue - batch.recordCount())
          throw new InvalidRequestException("Invalid allocated global offset in append response")
      } else if (data.globalBaseOffset() != -1) {
        throw new InvalidRequestException("Only INDEXED supplies a global offset")
      }
      new AppendResponse(status, batch, if (data.globalBaseOffset() == -1) OptionalLong.empty() else OptionalLong.of(data.globalBaseOffset()),
        through, data.coordinatorLeaderEpoch())
    case _ => throw new InvalidRequestException("Unexpected global sequence append response type")
  }
}
