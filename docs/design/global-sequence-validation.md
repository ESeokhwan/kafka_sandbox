<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Global sequence 장애 검증

[설계의 S01~S20](global-sequence.md#12-장애-시나리오와-기대-결과)을 구현 테스트에 연결한다.
실제 복제·네트워크·broker 재시작은 KafkaClusterTestKit 통합 테스트에서 확인하고,
정확한 callback 역전·로컬 append 실패·HW 경계는 제어 가능한 Future/Runtime 테스트에서 확인한다.
[운영 가이드](global-sequence-operations.md)는 이 검증 범위에 맞춘 사용·복구 절차다.

## 1. 18번에서 추가한 전체 경로

[GlobalSequenceFaultIntegrationTest][fault]는 controller 1개, broker 3개, 데이터 파티션 2개,
index partition 2개, RF=3/minISR=2에서 다음을 수행한다.

| 테스트 메서드 | 제어하는 장애와 확인 |
|---|---|
| `testIndexHwTimeoutAndResponseLossRecoverWithoutDuplicateMappings(false)` | Index follower fetcher만 중지하여 ISR을 유지한 채 HW를 고정. 실제 idempotent Produce는 데이터가 커밋되어도 timeout. Index LEO>HW 및 미커밋 범위가 공개되지 않음을 확인하고 fetcher 재개 후 동일 producer sequence 재전송 |
| 같은 메서드 `(true)` | 위 HW 지연 중 index leader 종료·새 leader 선출·기존 broker 재시작. 원본을 복구하여 동일 sequence 재전송에 physical offset 1을 반환하고 단일 매핑 유지 |
| `testCommittedDataRecoversAfterRestartAndSourceRoundTripFencesDelayedAppend` | 한 source indexer를 중지하고 다중 record 배치를 data HW까지 복제. 다른 파티션에 긴 최신 tail을 만든 뒤 A 종료→B 승격→A 복귀, index leader 교체. 첫 A의 Append를 실제 내부 RPC로 늦게 전달하여 fencing 확인. 다시 index leader 재시작 후 매핑과 읽기 불변 확인 |
| `testRetainedIndexReportsMissingDataAndRecreatedTopicStartsANewSequence` | 인덱싱된 원본의 DeleteRecords 후 앞 페이지의 cursor를 유지한 명시적 OFFSET_OUT_OF_RANGE. Index 이력은 보존. Topic 삭제·동일 이름 재생성 후 옛 UUID의 public/internal 요청 거절, 새 UUID 0번 시작 |

HW 지연은 임의 sleep 대신 해당 index partition의 replica fetcher 제거·재등록으로 제어한다.
`replica.lag.time.max.ms`는 테스트 기간보다 길게 두어 ISR 축소로 HW가 저절로 진행하는 경합을 막는다.
Election으로 epoch가 바뀌면 metadata가 fetcher를 재생성하므로 오래된 epoch의 fetcher를 되살리지 않는다.
토픽 생성 완료 응답의 UUID를 사용하고 각 broker에 metadata와 owner가 반영될 때까지 기다린다.

Response loss는 첫 완료 결과를 알고도 같은 producer sequence를 다시 전송하여 **클라이언트가
성공 여부를 모르는 재시도**를 재현한다. 실제 소켓에서 특정 성공 응답 패킷을 유실시키는 테스트는
아니다. 내부 pending/AlreadyIndexed 반환과 late callback은 별도 Runtime/Indexer 테스트에서 제어한다.
Broker lifecycle 테스트는 `shutdown/startup`을 사용한다. OS kill, 전원 손실, 디스크 torn write의 검증으로
해석하지 않는다. 로컬 append 실패와 replay rollback은 Runtime의 fault injection으로 검사한다.

### 독립적인 결과 검증

Fault 테스트의 oracle은 API 응답의 committed progress를 정답으로 사용하지 않는다.
현재 source leader의 실제 log segment를 읽어 HW 아래의 완전한 non-control physical batch와 값을
수집한다. 별도 KafkaConsumer로 실제 index log의 HW 아래 레코드를 읽고 다음을 대조한다.

- 보존된 physical batch마다 인덱스가 정확히 하나 존재한다.
- `(topic UUID, physical partition, physical base)`가 중복되지 않는다.
- Index log 순서의 global range가 0부터 연속되고 서로 겹치지 않는다.
- 같은 physical partition의 매핑 순서가 실제 source batch 순서와 같다.
- Last offset/count가 실제 배치와 일치하고 바로 다음 index record에 같은 UUID의 TopicMetadata가 있다.
- TopicMetadata의 next offset이 해당 allocation 끝과 일치하여 커밋된 할당 쌍이 완전하다.
- 모든 broker를 통한 global Fetch가 독립적으로 구성한 값·global offset 순서와 일치한다.
  `MaxBytes=1`로 완전 배치가 soft limit을 넘는 페이지, 배치 중간의 시작·종료, cursor 재개를 검사한다.
- 문서의 Java 조회 예제를 실제로 실행해 lookup과 두 격리 수준의 selected 범위·Base64 값·cursor를 비교한다.

[GlobalSequenceTransactionFetchIntegrationTest][transaction]는 commit/abort 양쪽에서 source A→B→A와
index leader 교체 뒤 해당 broker를 재시작하고 두 격리 수준의 읽기를 다시 확인하도록 확장했다.
앞의 열린 transaction은 뒤 파티션을 막고, abort는 global hole로 남으며, 같은 producer의 다음
transaction은 정상 반환된다. Control physical offset, 원본 CRC와 페이지 cursor도 확인한다.

[GlobalSequenceConsumerIntegrationTest][consumer]는 raw protocol 대신 public
`KafkaGlobalSequenceConsumer`와 README의 `GlobalSequenceConsumerExample`을 실제 broker에 연결한다.

- 독립적으로 읽은 index log와 producer의 physical metadata를 기준으로 압축 배치의 global/physical
  위치, null, header, timestamp와 source leader epoch를 비교한다.
- 한 배치짜리 page 사이에서 source/index leader를 이동하고, broker 하나를 중단한 bootstrap에서도
  최초 UUID로 범위를 이어 읽는다.
- Commit/abort 양쪽에서 pending page와 빈 abort page의 cursor를 확인한다.
- DeleteRecords 뒤 유효 prefix와 partial error를 함께 받고, 같은 이름으로 재생성된 토픽에 옛 UUID를
  적용하지 않는지 확인한다.
- Broker request metric으로 JoinGroup, Heartbeat, OffsetCommit이 발생하지 않고
  FetchGlobalSequence가 실제 발생하는지 확인한다.

## 2. S01~S20 추적 표

아래 메서드는 대표 진입점이다. 같은 클래스의 보조 경계 테스트도 함께 실행한다.

| ID | 정밀 단위/Runtime 검증 | 실제 broker 검증 |
|---|---|---|
| S01 | [Indexer][indexer] `testOnlyReadNextBatchAfterIndexCommitAndCoalesceWakeups`; [Shard][shard] `testPredecessorChecksAllowControlGapsButRejectEarlyOrStaleSubmissions` | [Coordinator integration][coordinator] `testAutomaticIndexingAcrossSourcePartitionsAndIndexLeaderChange`; [fault][fault] 독립 oracle |
| S02 | [Shard][shard] `testDeterministicInterleavingAgainstReferenceSequence`, `testCrossPartitionInterleavingAndPerTopicSequences` | [Coordinator integration][coordinator] acks=1/all 다중 파티션; [fault][fault] global 비중첩·물리 순서 |
| S03 | [Indexer][indexer] `testResumeCommittedProgressAndSkipControlOnlyReads` | [fault][fault] `testCommittedDataRecoversAfterRestartAndSourceRoundTripFencesDelayedAppend` |
| S04 | [Runtime][runtime] `testLocalAppendFailureRollsBackReplayedAllocation`, `testBufferedAppendFailureRollsBackAllTopicsAndRetryWaiters` | 로컬 append 실패는 Runtime에서 직접 주입 |
| S05 | [Runtime][runtime] `testTimeoutRetryUsesSameAllocationAndWaitsForCommit` (linger 0/5); [Indexer][indexer] `testAppendTimeoutRetriesIdenticalBatchPredecessorAndIdentity` | [fault][fault] HW 정지 후 복제 재개/leader 종료 두 경우 |
| S06 | [Shard][shard] `testPendingRetryReusesAllocationAndOldRetryNeedsNoMapping` | [fault][fault] 성공 응답을 사용하지 않은 동일 idempotent Produce 재전송 |
| S07 | [Shard][shard] `testLongHistoryRetainsOnlyLatestProgressAndPendingAllocations`, `testPendingRetryReusesAllocationAndOldRetryNeedsNoMapping` | 과거 매핑 cache 자체가 없는 설계. 이력 없는 중복 판단은 Shard에서 검증 |
| S08 | [Indexer][indexer] `testLateAppendCallbackCannotReadAfterFollowerTransition`; [Shard][shard] `testOwnershipIsRequiredAndPendingFenceBlocksBothOldAndNewOwner` | [fault][fault] source A→B→A 이후 첫 A의 Append 내부 RPC 지연 전달 |
| S09 | [Shard][shard] `testConditionalRegistrationIsIdempotentBeforeAndAfterCommit`, `testSupersededRegistrationCannotReclaimOwnership`; [Routing][routing] `testRegistrationRetryPreservesCasAndUuidAcrossCoordinatorEpochChange` | [Coordinator integration][coordinator] RPC routing/leader 교체. 응답 유실·CAS는 단위에서 제어 |
| S10 | [Runtime][runtime] `testLoadedPendingTailRetryDoesNotAppendAndWaitsForHighWatermark`; [Shard][shard] `testReplayRestoresCommittedPrefixAndMultiplePendingBatchesInOnePartition` | [fault][fault] 미커밋 index tail이 있는 leader 종료 및 재시작 |
| S11 | [Index reader][reader] `testLeadershipChangeDuringScanRejectsLateResult`; [Routing][routing] `testRemoteResponseAfterLeaderChangeIsDiscardedAndReadLocally`; [Indexer][indexer] `testLeaderChangeDuringGapRecheckRequiresAnotherCurrentBarrier` | [fault][fault] 복구와 index leader 변경 조합. Scan 도중 epoch 경계는 단위에서 제어 |
| S12 | [Shard][shard] `testLongHistoryRetainsOnlyLatestProgressAndPendingAllocations` | [fault][fault] 다른 파티션의 긴 최신 tail 아래에 있는 조용한 파티션의 progress 복구 |
| S13 | [Retention log][retention-log], [Retention manager][retention-manager], [Indexer][indexer] `testUnrecoverableGapStopsAfterBarrierAndRecheckWithoutJumpingToLogStart` | [Retention integration][retention] `testRetentionAndDeleteRecordsPreserveUnindexedDataAcrossRestartAndFollowerPromotion`, `testNewFollowerCanCatchUpPastExpiredIndexedPrefix`; [fault][fault] 삭제된 원본 읽기의 명시적 오류 |
| S14 | [Produce][produce] `testCompletedBeforeAndDuringRegistrationAndIndependentRanges`, `testConcurrentSubscriptionAndCommitNeverLoseCompletion` | 실제 Produce 전체 경로는 [coordinator][coordinator]와 [fault][fault], 정확한 등록 경합은 단위 |
| S15 | [Produce][produce] `testDeadlineDetachesOnlyExpiredWaiterAndKeepsProgress`; [Indexer][indexer] `testProduceTimeoutDoesNotCancelPendingIndexAppend` | [fault][fault] Produce timeout 이후 background 완료 및 원래 physical offset 재응답 |
| S16 | [Fetch manager][fetch] `testReverseCompletionsRespectGlobalOrderAndBoundTheReadWindow`, `testFrontFailureReturnsOnlyTheCompletedPrefix` | [fault][fault] 모든 broker에서 작은 페이지와 앞 원본 누락 확인. 응답 역전은 Future로 제어 |
| S17 | [Fetch manager][fetch] `testReadCommittedStopsAtTheFirstPendingGlobalRangeDespiteCompletedLaterPartitions`; [Data reader][data] LSO/marker 경계 | [Transaction integration][transaction] 열린 앞 transaction, source leader 이동 후 commit/abort 및 재시작 |
| S18 | [Fetch manager][fetch] `testAllAbortedPartialPageAndResumePreserveGlobalHoles`, `testPartialBatchSelectionKeepsOriginalRecordsAndExactCursor`; [Data reader][data] abort 범위·동일 producer 후속 commit | [Transaction integration][transaction] commit/abort, control 간격, page 재개; [fault][fault] 다중 record 배치의 중간 범위 |
| S19 | [Lookup APIs][lookup-api] `testUnknownUuidAndStaleRouteDoNotReturnMappings`; [Fetch APIs][fetch-api] `testUnknownUuidAndInvalidInputsNeverFetch` | [fault][fault] `testRetainedIndexReportsMissingDataAndRecreatedTopicStartsANewSequence` |
| S20 | [Produce][produce] `testMixedResponsesKeepPhysicalOffsetsAndOnlyWaitOnSuccessfulGlobalPartitions`; [Lookup APIs][lookup-api] `testTopicReadAuthorizationPrecedesRouting`; [Fetch APIs][fetch-api] `testTopicReadAclPrecedesLookupAndPhysicalWork` | 일반/혼합 Produce와 권한의 정밀 검증은 API 단위에서 수행 |

## 3. 재현 명령

저장소 루트, Java 17과 Gradle wrapper 기준이다. 새로운 18번 장애·재시작 테스트만 실행하려면:

```sh
./gradlew :examples:jar :examples:checkstyleMain :examples:spotbugsMain \
  :core:test --tests 'kafka.server.GlobalSequenceFaultIntegrationTest' \
  --tests 'kafka.server.GlobalSequenceTransactionFetchIntegrationTest' \
  -PmaxParallelForks=2 --max-workers=4 --console=plain
```

Public consumer와 관련 broker/API/client 회귀만 실행하려면:

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.*GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  :examples:jar :examples:checkstyleMain :examples:spotbugsMain \
  :core:test --tests 'kafka.server.GlobalSequence*IntegrationTest' \
  --tests 'kafka.server.GlobalSequenceFetchApisTest' \
  --tests 'kafka.server.GlobalSequenceLookupApisTest' \
  -PmaxParallelForks=1 --max-workers=4 --continue --console=plain
```

2026-09-16, C07 선별 회귀는 새 public consumer 실제 broker 시나리오 4개를 포함한 core 31개와
client 72개, **총 103개 통과, 실패/skip 0개**다. Clients/examples/core Checkstyle,
Clients/examples/core SpotBugs와 clients Javadoc도 통과했다. SASL/SSL 설정 전달은 client 단위에서
검증했으며 별도의 보안 listener cluster는 이 선별 회귀에 포함하지 않았다.

전체 global sequence 및 연결 지점 회귀 명령:

```sh
./gradlew :examples:jar :examples:checkstyleMain :examples:spotbugsMain \
  :core:test --tests 'kafka.server.GlobalSequence*Test' \
  --tests 'kafka.server.IndexRoutingManagerTest' --tests 'kafka.server.KafkaApisTest' \
  --tests 'kafka.network.RequestChannelTest' \
  --tests 'kafka.network.SocketServerTest.testResponseResourceReleaseOnSendAndAfterProcessorShutdown' \
  --tests 'kafka.network.SocketServerTest.testClientDisconnectionUpdatesRequestMetrics' \
  --tests 'kafka.network.SocketServerTest.processNewResponseException' \
  --tests 'kafka.network.SocketServerTest.processCompletedSendException' \
  --tests 'kafka.network.SocketServerTest.processDisconnectedException' \
  :global-sequence-coordinator:test \
  :coordinator-common:test --tests 'org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime*Test' \
  :storage:test --tests 'org.apache.kafka.storage.internals.log.TransactionIndexTest' \
  :clients:test --tests 'org.apache.kafka.common.protocol.ApiKeysTest' \
  --tests 'org.apache.kafka.common.requests.RequestResponseTest' \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```

2026-09-15, Java 17·Gradle 8.14.1에서 위 최종 선별 회귀는 **825개 통과, 실패/skip 0개**였다.
Core 581, global-sequence-coordinator 79, coordinator-common 102, storage 15, clients 48이며
관련 Checkstyle·SpotBugs도 통과했다. 새 fault 4건과 확장한 transaction 2건을 포함한다.

테스트 XML은 각 모듈의 `build/test-results/test`, HTML은 `build/reports/tests/test`에 생성된다.
실패한 core 테스트의 broker 로그는 `core/build/reports/testOutput`에 있다.
전체 Kafka 테스트 스위트나 장시간 soak/처리량 benchmark를 모두 통과했다는 의미는 아니다.
17번에서 변경 전 동작으로도 재현한 기존 SocketServer 실패 9건은
[당시 검증 기록](global-sequence.md#144-검증-기록)에 남겨 두고 위 선별 회귀와 구분한다.

[fault]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceFaultIntegrationTest.scala
[transaction]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceTransactionFetchIntegrationTest.scala
[consumer]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceConsumerIntegrationTest.scala
[coordinator]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceCoordinatorIntegrationTest.scala
[retention]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceRetentionIntegrationTest.scala
[retention-log]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceRetentionLogTest.scala
[retention-manager]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceRetentionManagerTest.scala
[indexer]: ../../core/src/test/scala/unit/kafka/server/GlobalSequencePartitionIndexerTest.scala
[shard]: ../../global-sequence-coordinator/src/test/java/org/apache/kafka/coordinator/globalsequence/GlobalSequenceCoordinatorShardTest.java
[runtime]: ../../global-sequence-coordinator/src/test/java/org/apache/kafka/coordinator/globalsequence/GlobalSequenceCoordinatorShardRuntimeTest.java
[routing]: ../../core/src/test/scala/unit/kafka/server/IndexRoutingManagerTest.scala
[reader]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceIndexReaderTest.scala
[data]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceDataReaderTest.scala
[produce]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceProduceTest.scala
[fetch]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceFetchManagerTest.scala
[lookup-api]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceLookupApisTest.scala
[fetch-api]: ../../core/src/test/scala/unit/kafka/server/GlobalSequenceFetchApisTest.scala
