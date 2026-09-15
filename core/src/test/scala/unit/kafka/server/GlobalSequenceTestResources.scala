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

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinatorConfig, GlobalSequenceResources}

import scala.jdk.CollectionConverters._

private[server] class GlobalSequenceTestResources(time: Time, extra: Map[String, AnyRef] = Map.empty) extends AutoCloseable {
  import GlobalSequenceCoordinatorConfig._
  val metrics = new Metrics()
  val config = new GlobalSequenceCoordinatorConfig(new AbstractConfig(CONFIG_DEF, (Map[String, AnyRef](
    MAX_PENDING_OPERATIONS_CONFIG -> Int.box(2), MAX_PENDING_PER_PARTITION_CONFIG -> Int.box(1),
    MAX_PRODUCE_WAITERS_CONFIG -> Int.box(2), FETCH_BUFFER_BYTES_CONFIG -> Int.box(48 * 1024 * 1024)) ++ extra).asJava, false))
  val resources = new GlobalSequenceResources(config, metrics, time)
  override def close(): Unit = { resources.close(); metrics.close() }
}
