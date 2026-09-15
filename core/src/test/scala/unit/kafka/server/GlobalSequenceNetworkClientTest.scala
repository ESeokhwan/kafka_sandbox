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

import org.apache.kafka.clients.MockClient
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, DisconnectException, UnsupportedVersionException}
import org.apache.kafka.common.message.{DescribeGlobalSequencePartitionRequestData, DescribeGlobalSequencePartitionResponseData}
import org.apache.kafka.common.requests.{DescribeGlobalSequencePartitionRequest, DescribeGlobalSequencePartitionResponse}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.util.concurrent.TimeUnit

@Timeout(15)
class GlobalSequenceNetworkClientTest {
  private val node = new Node(2, "localhost", 9002)
  private val topicId = new Uuid(1, 2)
  private def request: DescribeGlobalSequencePartitionRequest.Builder =
    new DescribeGlobalSequencePartitionRequest.Builder(new DescribeGlobalSequencePartitionRequestData().setTopicId(topicId).setPartition(0))
  private def response: DescribeGlobalSequencePartitionResponse =
    new DescribeGlobalSequencePartitionResponse(new DescribeGlobalSequencePartitionResponseData().setTopicId(topicId).setPartition(0))

  @Test
  def testCancelledWireRequestRetainsBudgetUntilActualCompletion(): Unit = {
    val time = new MockTime()
    val limits = new GlobalSequenceTestResources(time)
    val client = new MockClient(time)
    val transport = new GlobalSequenceNetworkClient(client, 1000, time, limits.resources, data = true)
    import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope.DATA_RPC
    try {
      transport.startup()
      val first = transport.send(node, request)
      org.apache.kafka.test.TestUtils.waitForCondition(() => client.requests().size() == 1, "RPC was not dispatched")
      first.cancel(false)
      assertEquals(1L, limits.resources.used(DATA_RPC))
      assertFutureThrows(classOf[org.apache.kafka.common.errors.ThrottlingQuotaExceededException], transport.send(node, request))
      client.respond(response)
      org.apache.kafka.test.TestUtils.waitForCondition(() => limits.resources.used(DATA_RPC) == 0, "RPC reservation was not released")
      val next = transport.send(node, request)
      transport.close()
      assertFutureThrows(classOf[CoordinatorNotAvailableException], next)
      assertEquals(0L, limits.resources.bytes(DATA_RPC))
    } finally { transport.close(); limits.close() }
  }

  @Test
  def testNetworkCompletionAndDisconnect(): Unit = {
    for (disconnected <- Seq(false, true)) {
      val time = new MockTime()
      val client = new MockClient(time)
      client.prepareResponseFrom(body => body.asInstanceOf[DescribeGlobalSequencePartitionRequest].data().topicId() == topicId,
        response, node, disconnected)
      val transport = new GlobalSequenceNetworkClient(client, 1000, time)
      try {
        transport.startup()
        val result = transport.send(node, request)
        if (disconnected) assertFutureThrows(classOf[DisconnectException], result)
        else assertEquals(response.data(), result.get(10, TimeUnit.SECONDS).asInstanceOf[DescribeGlobalSequencePartitionResponse].data())
      } finally transport.close()
      assertFalse(client.active())
    }
  }

  @Test
  def testUnsupportedVersionIsNotSilentlyAccepted(): Unit = {
    val time = new MockTime()
    val client = new MockClient(time)
    client.prepareUnsupportedVersionResponse(_ => true)
    val transport = new GlobalSequenceNetworkClient(client, 1000, time)
    try {
      transport.startup()
      assertFutureThrows(classOf[UnsupportedVersionException], transport.send(node, request))
    } finally transport.close()
  }

  @Test
  def testCloseFailsPendingRequestsAndRejectsNewRequests(): Unit = {
    val time = new MockTime()
    val client = new MockClient(time)
    client.delayReady(node, 10000)
    val transport = new GlobalSequenceNetworkClient(client, 1000, time)
    try {
      transport.startup()
      val pending = transport.send(node, request)
      transport.close()
      assertFutureThrows(classOf[CoordinatorNotAvailableException], pending)
      assertFutureThrows(classOf[CoordinatorNotAvailableException], transport.send(node, request))
    } finally transport.close()
    assertFalse(client.active())
  }

  @Test
  def testCloseBeforeStartupStillClosesNetworkResources(): Unit = {
    val client = new MockClient(new MockTime())
    val transport = new GlobalSequenceNetworkClient(client, 1000, new MockTime())
    assertFutureThrows(classOf[CoordinatorNotAvailableException], transport.send(node, request))
    transport.close()
    assertFalse(client.active())
    assertThrows(classOf[IllegalStateException], () => transport.startup())
  }
}
