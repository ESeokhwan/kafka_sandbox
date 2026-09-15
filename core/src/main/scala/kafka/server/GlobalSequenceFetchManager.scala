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

import kafka.network.RequestChannel.ResourceResponse
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope

import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.IsolationLevel.READ_UNCOMMITTED
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, NotCoordinatorException, TimeoutException}
import org.apache.kafka.common.message.FetchGlobalSequenceResponseData
import org.apache.kafka.common.requests.FetchGlobalSequenceResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ScheduledFuture}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** A sliding window bounds reads and completed out-of-order payloads; only the contiguous prefix can advance. */
class GlobalSequenceFetchManager(index: IndexRoutingManager, source: GlobalSequenceDataRouter,
                                 scheduler: Scheduler, time: Time, resources: GlobalSequenceResources = null) extends AutoCloseable {
  import GlobalSequenceFetch._
  private val pending = ConcurrentHashMap.newKeySet[CompletableFuture[FetchGlobalSequenceResponse]]()
  @volatile private var closed = false

  def readLocal(batch: PhysicalBatch, epoch: Int, deadlineNs: Long, isolation: IsolationLevel = READ_UNCOMMITTED): CompletableFuture[Data] =
    source.readLocal(batch, epoch, deadlineNs, isolation)

  private[server] def reserveDataResponse(key: PartitionKey): Option[GlobalSequenceResources#Lease] =
    Option(resources).map(_.acquire(Scope.DATA_RESPONSE, key, GlobalSequenceResources.BATCH_BYTES))

  def fetch(request: Request): CompletableFuture[FetchGlobalSequenceResponse] = new Session(request).start()

  private class Session(request: Request) {
    private var lease: Option[GlobalSequenceResources#Lease] = None
    private val result = new CompletableFuture[FetchGlobalSequenceResponse]()
    private val deadlineNs = time.nanoseconds() + request.lookup.timeoutMs().toLong * 1000000L
    private var timer: ScheduledFuture[_] = _
    private var lookup: CompletableFuture[IndexRoutingManager.RoutedResult[GlobalSequenceLookup.Result]] = _
    private var route: IndexRoutingManager.CoordinatorLocation = _
    private var mappings = Vector.empty[GlobalSequenceLookup.Mapping]
    private var response = new FetchGlobalSequenceResponseData().setTopicId(request.lookup.topicId())
      .setNextGlobalOffset(request.lookup.startOffset())
    private var running = Map.empty[Int, CompletableFuture[Data]]
    private var completed = Map.empty[Int, Either[Throwable, Data]]
    private var issued = 0
    private var consumed = 0
    private var bytes = 0
    private var draining = false

    def start(): CompletableFuture[FetchGlobalSequenceResponse] = synchronized {
      try lease = Option(resources).map(_.acquire(Scope.FETCH, request.lookup.topicId(), GlobalSequenceResources.FETCH_BYTES))
      catch { case NonFatal(error) => return CompletableFuture.failedFuture(error) }
      pending.add(result)
      result.whenComplete { (_, error) => synchronized {
        if (error != null) lease.foreach(_.finish(error))
        pending.remove(result)
        if (timer != null) timer.cancel(false)
        if (lookup != null) lookup.cancel(false)
        val work = running.values.toList
        running = Map.empty
        completed = Map.empty
        work.foreach(_.cancel(false))
      }}
      if (closed) result.completeExceptionally(new CoordinatorNotAvailableException("Global fetch manager is closed"))
      else try {
        timer = scheduler.scheduleOnce("global-sequence-fetch-timeout", () => synchronized {
          drain()
          if (!result.isDone) finish(Some(new TimeoutException("Global fetch deadline expired")))
        }, request.lookup.timeoutMs())
        if (result.isDone) timer.cancel(false)
        else {
          lookup = index.lookupIndex(request.lookup)
          lookup.whenComplete { (page, error) => synchronized {
            if (!result.isDone) {
              if (error != null) finish(Some(error))
              else {
                route = page.coordinator
                response = GlobalSequenceFetch.response(request, page.value.snapshot())
                mappings = page.value.mappings().asScala.toVector
                drain()
              }
            }
          }}
          if (result.isDone) lookup.cancel(false)
        }
        if (closed) result.completeExceptionally(new CoordinatorNotAvailableException("Global fetch manager is closed"))
      } catch { case NonFatal(error) => finish(Some(error)) }
      result
    }

    private def finish(error: Option[Throwable]): Unit = {
      if (result.isDone) return
      if (route != null && !index.isCurrent(new PartitionKey(request.lookup.topicId(), 0), route)) {
        response.batches().clear()
        response.setTransactionPending(false)
        response.setNextGlobalOffset(request.lookup.startOffset())
        complete(GlobalSequenceFetch.fail(response, new NotCoordinatorException("Index route changed during global fetch")))
      } else complete(error.fold(new FetchGlobalSequenceResponse(response))(GlobalSequenceFetch.fail(response, _)))
    }

    private def complete(page: FetchGlobalSequenceResponse): Unit = {
      val owned = new FetchGlobalSequenceResponse(page.data()) with ResourceResponse {
        override def release(): Unit = lease.foreach(_.finish(
          org.apache.kafka.common.protocol.Errors.forCode(page.data().errorCode()).exception()))
      }
      if (!result.complete(owned)) owned.release()
    }

    // All calls hold the session monitor. Immediate future completion re-enters without recursive draining.
    private def drain(): Unit = {
      if (draining || result.isDone || route == null) return
      draining = true
      try {
        var progress = true
        while (progress && !result.isDone) {
          progress = false
          while (completed.contains(consumed) && !result.isDone) {
            val value = completed(consumed)
            completed -= consumed
            value match {
              case Left(error) => finish(Some(error))
              case Right(data) if data.status == Pending =>
                response.setTransactionPending(true)
                finish(None)
              case Right(data) if data.status == Aborted =>
                response.setNextGlobalOffset(math.min(request.lookup.endOffset(), mappings(consumed).globalEndOffset()))
                consumed += 1
                progress = true
              case Right(data) =>
                val size = data.records.sizeInBytes()
                if (bytes > 0 && (bytes.toLong + size > request.maxBytes || bytes.toLong + size > MaxPayloadBytes)) finish(None)
                else {
                  val entry = GlobalSequenceFetch.fetched(request, mappings(consumed), data)
                  response.batches().add(entry)
                  response.setNextGlobalOffset(entry.selectedGlobalEndOffset())
                  bytes += size
                  consumed += 1
                  progress = true
                  if (bytes >= request.maxBytes) finish(None)
                }
            }
          }
          if (!result.isDone && consumed == mappings.size) finish(None)
          if (!result.isDone && time.nanoseconds() >= deadlineNs) finish(Some(new TimeoutException("Global fetch deadline expired")))
          while (!result.isDone && issued < mappings.size && issued < consumed + ParallelReads) {
            val position = issued
            issued += 1
            progress = true
            try {
              val read = source.read(mappings(position).batch(), deadlineNs, request.isolation)
              running += position -> read
              read.whenComplete { (data, error) => synchronized {
                running -= position
                if (!result.isDone) {
                  completed += position -> (if (error != null) Left(error) else Right(data))
                  drain()
                }
              }}
            } catch { case NonFatal(error) => completed += position -> Left(error) }
          }
        }
      } finally draining = false
    }
  }

  override def close(): Unit = {
    closed = true
    pending.asScala.foreach(_.completeExceptionally(new CoordinatorNotAvailableException("Global fetch manager is closed")))
    source.close()
  }
}
