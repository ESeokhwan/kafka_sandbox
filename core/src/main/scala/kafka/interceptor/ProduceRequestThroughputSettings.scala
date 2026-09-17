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
import org.apache.kafka.server.config.ProduceRequestThroughputConfigs

import java.nio.file.Path
import java.time.Duration

final case class ProduceRequestThroughputSettings(
  outputPath: Path,
  measurementIntervalMs: Long,
  rollOutInterval: Duration,
  maxRecordsPerFile: Long
)

object ProduceRequestThroughputSettings {
  def from(config: KafkaConfig): ProduceRequestThroughputSettings = ProduceRequestThroughputSettings(
    Path.of(config.getString(ProduceRequestThroughputConfigs.OUTPUT_PATH_CONFIG)),
    config.getLong(ProduceRequestThroughputConfigs.MEASUREMENT_INTERVAL_MS_CONFIG),
    Duration.ofMillis(config.getLong(ProduceRequestThroughputConfigs.ROLL_OUT_INTERVAL_MS_CONFIG)),
    config.getLong(ProduceRequestThroughputConfigs.MAX_RECORDS_PER_FILE_CONFIG)
  )
}
