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

import kafka.cluster.Partition
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.{CompletableFuture, Executor, RejectedExecutionException, ScheduledFuture, TimeUnit}
import scala.collection.{Map, mutable}
import scala.util.control.NonFatal

/** The actual append range, tied to the source lifetime that accepted it. Contains no record payload. */
private[server] case class GlobalSequenceAppendReceipt(source: Partition, topicId: Uuid, leaderEpoch: Int,
                                                      firstOffset: Long, lastOffset: Long)

/** Progress checks and subscription share a lock, so completion before/during registration is not lost. */
private[server] class GlobalSequenceIndexWaiters(scheduler: Scheduler, time: Time,
                                                sourceIsCurrent: () => Boolean, highWatermark: () => Long,
                                                resources: GlobalSequenceResources = null, key: AnyRef = new Object) {
  private class Waiter(val endOffset: Long, val deadlineNs: Long) {
    val future = new CompletableFuture[Void]()
    @volatile var timer: ScheduledFuture[_] = _
  }
  private val pending = mutable.Set.empty[Waiter]
  private var committedOffset = 0L
  private var closed: Option[Throwable] = None

  private def result(waiter: Waiter): Option[Errors] = {
    if (!sourceIsCurrent()) Some(Errors.NOT_LEADER_OR_FOLLOWER)
    else if (time.nanoseconds() >= waiter.deadlineNs) Some(Errors.REQUEST_TIMED_OUT)
    else if (waiter.endOffset <= committedOffset && waiter.endOffset <= highWatermark()) {
      // Reading HW and checking the source lifetime are separate volatile reads.
      // Recheck so a new leader/log cannot supply the HW for an old append receipt.
      Some(if (sourceIsCurrent()) Errors.NONE else Errors.NOT_LEADER_OR_FOLLOWER)
    } else None
  }

  def await(endOffset: Long, deadlineNs: Long): CompletableFuture[Void] = {
    val waiter = new Waiter(endOffset, deadlineNs)
    val outcome = synchronized {
      closed match {
        case Some(cause) => Some(cause)
        case None => result(waiter) match {
          case Some(error) => Some(error.exception())
          case None =>
            try {
              val lease = Option(resources).map(_.acquire(Scope.PRODUCE, key, 0))
              waiter.future.whenComplete((_, error) => lease.foreach(_.finish(error)))
              pending.add(waiter)
              None
            } catch { case NonFatal(error) => Some(error) }
        }
      }
    }
    outcome match {
      case Some(cause) => complete(waiter, cause)
      case None =>
        waiter.future.whenComplete { (_, _) =>
          synchronized { pending.remove(waiter) }
          val timer = waiter.timer
          if (timer != null) timer.cancel(false)
        }
        // Round up to avoid expiring early. A zero-delay MockScheduler may execute inline.
        val remainingNs = math.max(0L, deadlineNs - time.nanoseconds())
        try {
          waiter.timer = scheduler.scheduleOnce("global-sequence-produce-timeout", () => {
            val removed = synchronized { pending.remove(waiter) }
            if (removed) complete(waiter, Errors.REQUEST_TIMED_OUT.exception())
          }, (remainingNs + TimeUnit.MILLISECONDS.toNanos(1) - 1) / TimeUnit.MILLISECONDS.toNanos(1))
          if (waiter.future.isDone) waiter.timer.cancel(false)
        } catch {
          case NonFatal(error) => complete(waiter, error)
        }
    }
    waiter.future
  }

  /** Called on an index commit, recovered committed progress, and source HW notifications. */
  def advance(nextOffset: Long): Unit = {
    val completed = synchronized {
      committedOffset = math.max(committedOffset, nextOffset)
      if (closed.nonEmpty) List.empty
      else {
        val ready = pending.toList.flatMap(waiter => result(waiter).map(error => (waiter, error)))
        ready.foreach { case (waiter, _) => pending.remove(waiter) }
        ready
      }
    }
    completed.foreach { case (waiter, error) => complete(waiter, error.exception()) }
  }

  private def complete(waiter: Waiter, cause: Throwable): Unit = {
    if (cause == null) waiter.future.complete(null)
    else waiter.future.completeExceptionally(cause)
  }

  /** Partition listeners may hold partition locks. Fail futures on a worker, never in that listener. */
  def close(cause: Throwable, executor: Executor): Unit = {
    val abandoned = synchronized {
      closed = Some(cause)
      val result = pending.toList
      pending.clear()
      result
    }
    if (abandoned.nonEmpty) {
      val completion: Runnable = () => abandoned.foreach(waiter => complete(waiter, cause))
      try executor.execute(completion)
      catch { case _: RejectedExecutionException => CompletableFuture.runAsync(completion) }
    }
  }

  private[server] def size: Int = synchronized { pending.size }
}

private[server] object GlobalSequenceProduce {
  /** Preserve ordinary/data-error results and physical offsets while joining successful global partitions. */
  def awaitIndexes(responses: Map[TopicIdPartition, PartitionResponse],
                   receipts: Map[TopicIdPartition, GlobalSequenceAppendReceipt],
                   awaitIndex: GlobalSequenceAppendReceipt => CompletableFuture[Void],
                   callback: Map[TopicIdPartition, PartitionResponse] => Unit): Unit = {
    val waits = responses.iterator.collect {
      case (partition, response) if response.error == Errors.NONE && receipts.contains(partition) =>
        val future = try awaitIndex(receipts(partition)) catch {
          case NonFatal(error) => CompletableFuture.failedFuture[Void](error)
        }
        future.handle[Void] { (_, error) =>
          if (error != null) response.error = Errors.maybeUnwrapException(error) match {
            case _: FencedLeaderEpochException | _: NotLeaderOrFollowerException => Errors.NOT_LEADER_OR_FOLLOWER
            case _: GlobalSequenceSourceReader.SourceLogGapException => Errors.KAFKA_STORAGE_ERROR
            case cause => Errors.forException(cause)
          }
          null
        }
    }.toArray
    CompletableFuture.allOf(waits: _*).thenRun(() => callback(responses))
  }
}
