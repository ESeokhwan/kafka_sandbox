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
package kafka.interceptor.util

import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class PrintableMessageCounterTest {
  @Test
  def testCallbackFailureDoesNotDisableLaterCommits(): Unit = {
    var currentTimeMs = 1_000L
    val counter = new PrintableMessageCounter(1_000L, () => currentTimeMs)
    counter.increaseCounter(1)
    counter.tryCommit((_, _, _, _) => ())

    currentTimeMs = 2_001L
    assertThrows(classOf[RuntimeException], () =>
      counter.tryCommit((_, _, _, _) => throw new RuntimeException("failed")))

    currentTimeMs = 3_002L
    var committed = false
    counter.tryCommit((_, _, _, _) => committed = true)

    assertTrue(committed)
  }
}
