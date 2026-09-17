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
package kafka.interceptor

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.server.config.ProduceRequestThroughputConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.nio.file.Path
import java.time.Duration

class ProduceRequestThroughputSettingsTest {
  @Test
  def testBuildsSettingsFromBrokerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ProduceRequestThroughputConfigs.OUTPUT_PATH_CONFIG, "monitoring/throughput.csv")
    props.setProperty(ProduceRequestThroughputConfigs.MEASUREMENT_INTERVAL_MS_CONFIG, "5000")
    props.setProperty(ProduceRequestThroughputConfigs.ROLL_OUT_INTERVAL_MS_CONFIG, "60000")
    props.setProperty(ProduceRequestThroughputConfigs.MAX_RECORDS_PER_FILE_CONFIG, "500")

    val settings = ProduceRequestThroughputSettings.from(KafkaConfig.fromProps(props))

    assertEquals(Path.of("monitoring/throughput.csv"), settings.outputPath)
    assertEquals(5_000L, settings.measurementIntervalMs)
    assertEquals(Duration.ofMinutes(1), settings.rollOutInterval)
    assertEquals(500L, settings.maxRecordsPerFile)
  }

  @Test
  def testRejectsNonPositiveMeasurementInterval(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(ProduceRequestThroughputConfigs.MEASUREMENT_INTERVAL_MS_CONFIG, "0")

    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }
}
