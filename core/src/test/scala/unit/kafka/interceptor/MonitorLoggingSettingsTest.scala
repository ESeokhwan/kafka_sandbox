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
import moniq.writer.{BatchPolicy, FlushPolicy}
import moniq.writer.strategy.FileMonitorLogWriteStrategy.Format
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.server.config.MonitorLoggingConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertInstanceOf, assertThrows}
import org.junit.jupiter.api.Test

import java.nio.file.Path
import java.time.Duration

class MonitorLoggingSettingsTest {
  @Test
  def testBuildsIndependentSettingsForEachMonitorInterceptor(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_OUTPUT_PATH_CONFIG, "monitoring/plain.log")
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_ROLL_OUT_INTERVAL_MS_CONFIG, "60000")
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_MAX_LOGS_PER_FILE_CONFIG, "500")
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_FORMAT_CONFIG, "READ_FRIENDLY")
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_BATCH_SIZE_CONFIG, "10")
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_FLUSH_INTERVAL_MS_CONFIG, "1000")
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_PREPROCESSING_WORKER_COUNT_CONFIG, "2")
    props.setProperty(MonitorLoggingConfigs.JSON_BASED_MONITOR_LOGGING_OUTPUT_PATH_CONFIG, "monitoring/json.log")

    val config = KafkaConfig.fromProps(props)
    val monitorSettings = MonitorLoggingSettings.monitorLogging(config)
    val jsonSettings = MonitorLoggingSettings.jsonBasedMonitorLogging(config)

    assertEquals(Path.of("monitoring/plain.log"), monitorSettings.outputPath)
    assertEquals(Duration.ofMinutes(1), monitorSettings.rollOutInterval)
    assertEquals(500L, monitorSettings.maxLogsPerFile)
    assertEquals(Format.READ_FRIENDLY, monitorSettings.format)
    assertEquals(10, assertInstanceOf(classOf[BatchPolicy.FixedSize], monitorSettings.batchPolicy).size())
    assertEquals(Duration.ofSeconds(1), assertInstanceOf(classOf[FlushPolicy.After], monitorSettings.flushPolicy).timeout())
    assertEquals(2, monitorSettings.preprocessingWorkerCount)

    assertEquals(Path.of("monitoring/json.log"), jsonSettings.outputPath)
    assertEquals(Duration.ZERO, jsonSettings.rollOutInterval)
    assertEquals(1_000_000L, jsonSettings.maxLogsPerFile)
    assertEquals(Format.COMMA_SEPARATED, jsonSettings.format)
    assertInstanceOf(classOf[BatchPolicy.Unbounded], jsonSettings.batchPolicy)
    assertInstanceOf(classOf[FlushPolicy.Disabled], jsonSettings.flushPolicy)
    assertEquals(1, jsonSettings.preprocessingWorkerCount)
  }

  @Test
  def testRejectsUnsupportedMonitorLogFormat(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 8181)
    props.setProperty(MonitorLoggingConfigs.MONITOR_LOGGING_FORMAT_CONFIG, "JSON")

    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }
}
