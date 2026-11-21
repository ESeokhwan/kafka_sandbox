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
import moniq.writer.{BatchPolicy, FlushPolicy}
import moniq.writer.strategy.FileMonitorLogWriteStrategy.Format
import org.apache.kafka.server.config.MonitorLoggingConfigs

import java.nio.file.Path
import java.time.Duration

final case class MonitorLoggingSettings(
  outputPath: Path,
  rollOutInterval: Duration,
  maxLogsPerFile: Long,
  format: Format,
  batchPolicy: BatchPolicy,
  flushPolicy: FlushPolicy,
  preprocessingWorkerCount: Int
)

object MonitorLoggingSettings {
  def monitorLogging(config: KafkaConfig): MonitorLoggingSettings =
    fromConfig(config, MonitorLoggingConfigs.MONITOR_LOGGING_PREFIX)

  def jsonBasedMonitorLogging(config: KafkaConfig): MonitorLoggingSettings =
    fromConfig(config, MonitorLoggingConfigs.JSON_BASED_MONITOR_LOGGING_PREFIX)

  private def fromConfig(config: KafkaConfig, prefix: String): MonitorLoggingSettings = {
    val batchSize = config.getInt(prefix + MonitorLoggingConfigs.BATCH_SIZE_SUFFIX)
    val flushIntervalMs = config.getLong(prefix + MonitorLoggingConfigs.FLUSH_INTERVAL_MS_SUFFIX)
    MonitorLoggingSettings(
      Path.of(config.getString(prefix + MonitorLoggingConfigs.OUTPUT_PATH_SUFFIX)),
      Duration.ofMillis(config.getLong(prefix + MonitorLoggingConfigs.ROLL_OUT_INTERVAL_MS_SUFFIX)),
      config.getLong(prefix + MonitorLoggingConfigs.MAX_LOGS_PER_FILE_SUFFIX),
      Format.valueOf(config.getString(prefix + MonitorLoggingConfigs.FORMAT_SUFFIX)),
      if (batchSize == 0) BatchPolicy.unbounded() else BatchPolicy.fixedSize(batchSize),
      if (flushIntervalMs == 0) FlushPolicy.disabled() else FlushPolicy.after(Duration.ofMillis(flushIntervalMs)),
      config.getInt(prefix + MonitorLoggingConfigs.PREPROCESSING_WORKER_COUNT_SUFFIX)
    )
  }
}
