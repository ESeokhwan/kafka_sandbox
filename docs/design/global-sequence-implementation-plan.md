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

# Global sequence topic 구현 계획

작성 기준: 2026-09-15, `proj/globally_ordered_topic/develop_v4`, 16번 구현 커밋 기준.
출발점은 Kafka 4.1.1의 vanilla 코드인 `be816b82d2`다.

기존 1~18번 구현 순서를 유지하여 완료한 작업과 남은 작업을 커밋 단위로 정리한다.
**1~16번은 구현 완료, 17~18번은 미구현이다.** 완료한 단계의 설명은 현재까지의
후속 보완을 포함한 구현 상태를 기준으로 한다. 남은 단계의 커밋 제목과 새 API 이름은 계획안이다.
이 문서의 단계 번호는 [상세 설계 문서](global-sequence.md)의 장 번호와 별개다.

## 목표와 공통 계약

1차 구현 범위는 **Produce, 순차 인덱싱, 장애 복구, global offset 기반 조회·데이터 읽기**다.
트랜잭션 데이터의 `READ_UNCOMMITTED`와 `READ_COMMITTED`도 포함한다.

- 토픽 생성 시 `global.sequence.enabled=true`로 활성화한다. 기존 토픽의 전환은 지원하지 않는다.
- 같은 데이터 파티션은 실제 로그에 저장된 순서대로 인덱싱한다. 다음 배치는 앞 배치의 인덱스 커밋 이후에 진행한다.
- 토픽 UUID별로 global offset을 0부터 할당한다. 서로 다른 데이터 파티션의 배치는 이 global 순서에 합류한다.
- 완전한 데이터 배치가 data HW 아래에 있을 때만 인덱싱한다. Control batch에는 global offset을 할당하지 않는다.
- 신규 할당 여부는 coordinator write 실행 시의 committed/pending 상태와 소유권으로 판단한다.
- 완료된 physical prefix의 재시도에는 새 번호를 할당하지 않는다. 과거 배치 cache miss를 중복 판정용 scan으로 보내지 않는다.
- Timeout은 대기의 종료다. 이미 수락한 인덱스 write나 백그라운드 인덱싱을 취소하지 않는다.
- 복구는 등록 barrier 이후 확인한 committed progress를 기준으로 한다. 고정된 개수의 최신 인덱스만 읽어 진행 위치를 추정하지 않는다.
- 미인덱싱 원본을 모든 replica에서 보존한다. 복구할 수 없는 누락을 발견하면 위치를 건너뛰지 않고 중단한다.
- Produce 응답과 기존 Kafka Fetch의 offset은 physical offset이다. Global 읽기는 별도 API로 제공한다.

Global Consumer Group, 데이터 compaction, 기존 토픽 이력 전환, 인덱스 이력 GC,
원격 계층만으로 수행하는 인덱싱 복구, mixed-version rolling upgrade는 이 18단계의 범위 밖이다.

## 전체 단계와 현재 상태

| 번호 | 작업 | 상태 | 구현 커밋 |
|---|---|---|---|
| 1 | 설계 계약 확정 | 완료 | `a9ca1fd6c3` |
| 2 | Coordinator 모듈과 활성화 설정 | 완료 | `263e747f28` |
| 3 | 인덱스 레코드 스키마와 serde | 완료 | `640f946a8d` |
| 4 | Coordinator Runtime write context와 상태 hook | 완료 | `c0c8ce3fb7` |
| 5 | 순서·중복·pending을 관리하는 할당 상태 | 완료 | `55b477ac42` |
| 6 | 브로커 서비스와 내부 인덱스 토픽 lifecycle | 완료 | `fdfd8cfded` |
| 7 | 소유권 등록·진행 조회·append 내부 RPC | 완료 | `05ef987778` |
| 8 | 인덱스 리더 라우팅과 네트워크 재시도 | 완료 | `3fa8418435` |
| 9 | HW 아래 원본 배치를 읽는 Source Reader | 완료 | `a57caf4597` |
| 10 | 데이터 파티션별 자동 순차 Indexer | 완료 | `7bc04e9a28` |
| 11 | Source/index leader 변경과 장애 복구 | 완료 | `8541560170` |
| 12 | 모든 replica의 미인덱싱 원본 보존 | 완료 | `7fcff61d64` |
| 13 | Produce 응답의 인덱스 커밋 대기 | 완료 | `1fe98f1b68` |
| 14 | Global offset 범위의 인덱스 조회 API | 완료 | `d9d6f7179b` |
| 15 | Global offset 기반 데이터 Fetch API | 완료 | `0e85c08e77` |
| 16 | Global Fetch의 트랜잭션 격리 | 완료 | 본 커밋 |
| 17 | 작업량·메모리 제한, backpressure, 상세 지표 | 미구현 | 예정 |
| 18 | 종합 장애 테스트와 사용·운영 문서 | 미구현 | 예정 |

| 마일스톤 | 단계 | 완료 조건 | 현재 상태 |
|---|---|---|---|
| A. 인덱싱 기반 | 1~9 | 저장 형식, coordinator, RPC, 라우팅, 원본 Reader 연결 | 완료 |
| B. 쓰기와 복구 | 10~13 | 자동 인덱싱·복구·원본 보존·Produce 대기 연결 | 완료 |
| C. Global 읽기 | 14~16 | 범위 조회, 데이터 Fetch, 두 격리 수준 제공 | 완료 |
| D. 자원 제한과 종합 검증 | 17~18 | 과부하 제어, 관측 지표, 장애 검증과 사용 문서 | 미구현 |

## 완료한 단계

### 1. 설계 계약 확정

상태: 완료 · 커밋: `a9ca1fd6c3`

- PhysicalBatchId, global range, data/index HW, committed progress, pending allocation을 정의한다.
- 데이터 파티션별 순서와 토픽별 global 순서의 관계를 확정한다.
- 재시도·timeout·fencing·rollback·복구·retention의 정확성 조건을 정한다.
- Produce의 acks별 계약과 global lookup/fetch 및 트랜잭션 격리 계약을 정한다.
- 장애 시나리오 S01~S20을 후속 구현의 인수 기준으로 기록한다.

산출물: [global-sequence.md](global-sequence.md).
검증 기준: 뒤 단계의 코드와 테스트가 같은 offset·커밋·복구 계약을 사용한다.

### 2. Coordinator 모듈과 활성화 설정

상태: 완료 · 커밋: `263e747f28` · 의존: 1

- `global-sequence-coordinator` 모듈을 추가하고 Gradle 빌드에 연결한다.
- `global.sequence.enabled` 및 coordinator의 정적 브로커 설정을 정의한다.
- 생성 시 활성화만 허용하고, 활성화된 데이터 토픽에는 명시적인 `cleanup.policy=delete`를 요구한다.
- 기존 토픽의 활성화/비활성화, compaction 활성화, 필수 설정 override 삭제를 거절한다.
- 내부 토픽에 데이터 토픽용 활성화 설정을 적용하지 못하도록 한다.

주요 위치: `settings.gradle`, `build.gradle`, `GlobalSequenceCoordinatorConfig`,
`LogConfig`, controller 설정 검증 경로.
검증 기준: 설정 기본값·범위와 생성/변경 제한을 확인하고 일반 토픽의 기존 설정 동작을 유지한다.

### 3. 인덱스 레코드 스키마와 serde

상태: 완료 · 커밋: `640f946a8d` · 의존: 2

- `BatchIndex`, `TopicMetadata`, `IndexerFence`의 key/value v0 스키마를 정의한다.
- Kafka MessageGenerator로 메시지를 생성하고 coordinator record serde와 생성 helper를 구현한다.
- BatchIndex에는 global base와 physical partition/base/last/count를 저장한다.
- TopicMetadata에는 다음 global offset, IndexerFence에는 source owner와 등록 식별 정보를 저장한다.
- UUID, 범위, 개수, overflow 및 잘못된 직렬화 입력을 검사한다.

주요 위치: `global-sequence-coordinator/src/main/resources/common/message`,
`GlobalSequenceCoordinatorRecordSerde`, `GlobalSequenceCoordinatorRecordHelpers`.
검증 기준: round-trip, 고정 wire fixture, 잘린 레코드·알 수 없는 버전/type 처리.

### 4. Coordinator Runtime write context와 상태 hook

상태: 완료 · 커밋: `c0c8ce3fb7` · 의존: 1~3

- Write 이벤트가 실제 실행되는 시점의 `highWatermark`와 `leaderEpoch`를 context로 전달한다.
- `onHighWatermarkUpdated`와 `onRollback` hook을 제공한다.
- HW 갱신을 write Future 성공 전에 반영하고 실제 rollback과 단순 timeout을 구분한다.
- 이전 shard의 늦은 알림이 새 shard의 상태를 바꾸지 않도록 한다.
- 빈 레코드 결과도 선행 pending write의 커밋을 기다리는 Runtime 계약을 유지한다.

주요 위치: `CoordinatorRuntime`, `CoordinatorWriteContext`, `CoordinatorShard`, `SnapshottableCoordinator`.
검증 기준: 실행 시점 context, HW/rollback 순서, stale callback과 hook 실패, 기존 coordinator 회귀 테스트.

### 5. 순서·중복·pending을 관리하는 할당 상태

상태: 완료 · 커밋: `55b477ac42` · 의존: 3, 4

- `GlobalSequenceCoordinatorShard.prepareAppend`에서 소유권, predecessor, 범위와 진행 상태를 검사한다.
- 신규 배치에는 BatchIndex와 TopicMetadata를 하나의 atomic 결과로 반환한다.
- Replay로 speculative 상태를 갱신하고 index HW로 committed 상태를 전진시킨다.
- Committed prefix의 재시도는 `ALREADY_INDEXED`, 같은 pending 배치는 기존 할당에 대기를 연결한다.
- 순서 오류와 소유권 오류를 신규 할당으로 처리하지 않는다.
- Replay/rollback 시 global 연속성, physical 순서, 할당 쌍과 pending tail의 일관성을 검증한다.

주요 위치: `GlobalSequenceCoordinatorShard`.
검증 기준: 파티션 간 범위 비중첩, 파티션 내 순서, pending 재시도, HW 전 성공 금지, rollback·replay 복원.

### 6. 브로커 서비스와 내부 인덱스 토픽 lifecycle

상태: 완료 · 커밋: `fdfd8cfded` · 의존: 2~5

- Coordinator 인터페이스·서비스·shard builder를 구현하고 Runtime, loader, writer를 연결한다.
- `BrokerServer` 및 metadata publisher에서 서비스 시작, 인덱스 리더 load/unload와 종료를 관리한다.
- 활성 데이터 토픽이 있으면 `__global_sequence_index`의 생성을 요청하고 metadata를 확인한다.
- Topic UUID를 고정된 인덱스 파티션 함수로 매핑하고 인덱스 파티션 수 변경을 거절한다.
- 내부 인덱스 이력을 보존하고 관측한 내부 토픽의 삭제·UUID 교체를 오류로 처리한다.
- 시작 실패와 브로커 종료에서도 executor, timer, loader, metrics 자원을 해제한다.

주요 위치: `GlobalSequenceCoordinatorService`, `BrokerServer`, `BrokerMetadataPublisher`, 내부 토픽 생성 경로.
검증 기준: 내부 토픽 생성·설정 보호, load/unload, startup 실패 정리, 브로커 재시작 후 상태 복원.

### 7. 소유권 등록·진행 조회·append 내부 RPC

상태: 완료 · 커밋: `05ef987778` · 의존: 5, 6

- `RegisterGlobalSequenceIndexer`, `DescribeGlobalSequencePartition`, `AppendGlobalSequenceIndex`를 추가한다.
- 브로커 내부 요청에 `CLUSTER_ACTION` 권한과 실행 시점 source/coordinator epoch 검사를 적용한다.
- 등록은 expected generation 기반 CAS이며 동일 registration UUID의 재시도는 같은 fence에 연결한다.
- 등록 성공은 선행 write와 fence의 커밋 barrier를 보장한다.
- Describe는 committed physical progress와 등록 CAS에 필요한 최신 owner를 구분하여 반환한다.
- Append의 실행 오류와 `INDEXED/ALREADY_INDEXED/FENCED/OWNER_NOT_COMMITTED/OUT_OF_ORDER`를 구분한다.

주요 위치: `clients`의 요청/응답 스키마, `GlobalSequenceApis`, `GlobalSequenceProtocol`, coordinator 서비스.
검증 기준: 프로토콜 round-trip, ACL, 오래된 epoch·CAS 거절, 등록 재시도와 커밋 전 응답 금지.
현재 내부 API 번호 93~95는 이 브랜치의 번호이며 upstream 예약 번호가 아니다.

### 8. 인덱스 리더 라우팅과 네트워크 재시도

상태: 완료 · 커밋: `3fa8418435` · 의존: 6, 7

- `IndexRoutingManager`가 현재 metadata에서 인덱스 리더와 inter-broker endpoint를 찾는다.
- 로컬 coordinator는 직접 호출하고 원격 coordinator는 `GlobalSequenceNetworkClient`로 호출한다.
- Index topic UUID, partition, broker/leader epoch와 endpoint를 캡처하고 응답 적용 시 재검증한다.
- 일시적인 실패는 하나의 deadline 안에서 backoff하며 같은 논리적 요청으로 재시도한다.
- CAS 거절과 fencing을 자동 소유권 재획득으로 숨기지 않는다.
- 대기 취소와 네트워크 종료는 coordinator가 수락한 write의 rollback으로 이어지지 않는다.

주요 위치: `IndexRoutingManager`, `GlobalSequenceNetworkClient`.
검증 기준: local/remote 경로, 응답 유실, stale route, 리더 변경, deadline·취소·종료와 인증/프로토콜 실패.

### 9. HW 아래 원본 배치를 읽는 Source Reader

상태: 완료 · 커밋: `a57caf4597` · 의존: 2, 5, 8

- Source topic UUID, Partition, leader epoch와 읽기 위치를 검증한다.
- 실제 data HW를 캡처하고 그 아래에 완전히 들어온 배치만 선택한다.
- 제한된 크기로 원본 로그를 읽고 CRC, record count와 physical offset 연속성을 검사한다.
- Control batch는 global 할당 없이 순회하고 HW를 기다려야 하는 상태를 구분한다.
- 필요한 로컬 원본이 사라졌으면 `SourceLogGapException`으로 복구 판단에 넘긴다.
- 원본 I/O는 공유 worker에서 수행하고 partition listener나 coordinator write 안에서 scan하지 않는다.

주요 위치: `GlobalSequenceSourceReader`.
검증 기준: 배치 중간 HW, 큰 배치, control-only 구간, 원본 손상·누락, 읽기 중 UUID/epoch/log 변경.

### 10. 데이터 파티션별 자동 순차 Indexer

상태: 완료 · 커밋: `7bc04e9a28` · 의존: 7~9

- 활성화된 각 로컬 데이터 리더에 하나의 `GlobalSequencePartitionIndexer`를 시작한다.
- 등록 barrier 뒤에 committed progress를 읽고 다음 physical 위치부터 순회한다.
- 파티션당 하나의 논리적 append만 진행하고 커밋 확인 후 다음 배치를 읽는다.
- HW 알림은 작업을 예약하며 여러 파티션이 공유 worker pool을 사용한다.
- Timeout 재시도에서는 배치, predecessor, owner와 registration UUID를 유지한다.
- Follower 전환, 삭제, 실패 및 브로커 종료 시 해당 Indexer의 수명을 끝낸다.

주요 위치: `GlobalSequencePartitionIndexer`, `GlobalSequenceIndexerManager`, metadata publisher.
검증 기준: 순차 진행, 파티션 간 독립성, HW 알림 경합, 동일 요청 재시도, source lifetime 종료.

### 11. Source/index leader 변경과 장애 복구

상태: 완료 · 커밋: `8541560170` · 의존: 6~10

- 새 데이터 리더는 등록 barrier 후 authoritative committed progress에서 재개한다.
- 인덱스 리더가 바뀌면 같은 등록 요청으로 barrier를 재확인하고 새 route의 progress를 사용한다.
- Source gap과 `OUT_OF_ORDER`는 확정 진행 상태를 다시 읽어 판단한다.
- 이미 처리된 pending 배치는 제거하고, 미처리 배치는 같은 식별 정보로 재시도한다.
- 재확인 후에도 누락이나 모순이 남으면 중단하며 log-start로 임의 이동하지 않는다.
- 데이터 리더 A→B→A에서도 첫 A의 callback이나 Indexer 수명을 재사용하지 않는다.

주요 위치: `GlobalSequencePartitionIndexer`, `GlobalSequenceIndexerManager`, coordinator/loader 복구 경로.
검증 기준: 데이터만 커밋된 tail 복구, source/index leader 이동, HW 지연, stale callback, 복구 불가능한 gap.

### 12. 모든 replica의 미인덱싱 원본 보존

상태: 완료 · 커밋: `7fcff61d64` · 의존: 9~11

- `UnifiedLog`에 확인된 indexed physical prefix 기반 삭제 경계를 적용한다.
- 새로 열린 활성 로그는 완료 위치를 확인할 때까지 보수적으로 보존한다.
- 시간·크기 retention, DeleteRecords, log-start 전파와 tiered local 삭제 경로에 경계를 적용한다.
- Leader뿐 아니라 follower와 future replica도 각자 committed progress를 확인한다.
- 진행 조회가 지연되거나 실패해도 보존 경계를 임의로 풀지 않는다.
- 뒤처진 replica의 초기화가 미인덱싱 prefix를 건너뛰지 않도록 재시도한다.

주요 위치: `UnifiedLog`, `GlobalSequenceRetentionManager`, replica fetch 경로.
검증 기준: 미인덱싱 배치 보존, 커밋 후 삭제 허용, 재시작·승격, future replica, remote/local 삭제 및 UUID 교체.

### 13. Produce 응답의 인덱스 커밋 대기

상태: 완료 · 커밋: `1fe98f1b68` · 의존: 10~12

- `ReplicaManager`가 실제 append 결과의 first/last physical offset과 source lifetime을 대기 대상으로 전달한다.
- `acks=1/all`은 기존 데이터 응답 조건에 더해 현재 data HW와 인덱스 커밋을 확인한다.
- `acks=0`에는 응답용 waiter를 만들지 않고 백그라운드 인덱싱을 유지한다.
- 진행 확인과 waiter 등록을 동기화하여 등록 전후의 완료 알림을 놓치지 않는다.
- 트랜잭션 검증·append·데이터 복제·인덱스 대기에 하나의 broker deadline을 사용한다.
- Timeout은 waiter만 해제한다. 리더 변경·실패·삭제는 옛 요청을 다른 source lifetime에 연결하지 않는다.
- 기존 physical offset 응답, 일반/global 혼합 파티션별 결과, ACL과 quota 처리를 유지한다.

주요 위치: `GlobalSequenceProduce`, `ReplicaManager`, `GlobalSequencePartitionIndexer`, `GlobalSequenceIndexerManager`.
검증 기준: 실제 append 범위, 완료/등록 경합, source HW 지연, timeout 후 인덱싱 지속,
리더 교체, 혼합 요청과 `acks=0/1/all`. 실제 브로커에서 Produce 성공 직후 committed index를 확인한다.

### 14. Global offset 범위의 인덱스 조회 API

상태: 완료 · 의존: 3~8, 11

구현 커밋: `feat: look up committed global sequence ranges`

**목표:** topic UUID와 global 범위를 받아 커밋된 physical 매핑을 페이지 단위로 조회한다.

- `[globalStartOffset, globalEndOffsetExclusive)`와 페이지 제한을 받는 요청/응답을 정의한다.
- 해당 범위와 겹치는 BatchIndex를 global 순서로 반환한다. 배치 중간의 시작·끝도 표현한다.
- 페이지 내에서 동일한 committed snapshot 경계를 사용하고 committed global end와 다음 조회 위치를 반환한다.
- Pending 할당과 index HW 밖의 레코드를 노출하지 않는다.
- 음수, 역전 범위, 빈 범위, committed end와 같거나 큰 시작 위치의 처리를 구현한다.
- 데이터 토픽 READ 권한과 요청 제한을 적용하고 기존 인덱스 리더 라우팅에 연결한다.
- 과거 매핑 조회에 scan이 필요하면 읽기량과 실행 시간을 제한하고 worker에서 수행한다.
- Scan 결과의 UUID·leader epoch·읽기 경계를 적용 전에 재검증한다. Cache가 없어도 조회 정확성을 유지한다.
- 전체 인덱스 이력을 무제한 메모리 맵에 올리는 방식에 의존하지 않는다.

주요 변경 위치: `clients` 프로토콜 정의와 요청/응답 클래스, `GlobalSequenceApis`,
coordinator 조회 경로, `IndexRoutingManager`, 필요 시 별도 인덱스 읽기 컴포넌트.

완료 기준: 커밋된 global 범위를 physical 배치로 조회하고 페이지를 끝까지 따라가도
매핑이 중복·누락되지 않는다. Cache hit/miss와 재시작·리더 변경 여부가 조회 결과를 바꾸지 않는다.
단위·통합 테스트로 미커밋 tail 비노출, 범위 경계, pagination, ACL과 stale scan을 검증한다.

검증: 조회 관련 단위·통합 테스트 173개와 기존 Kafka API/global sequence 회귀 테스트 586개 통과.
실제 브로커의 로컬·원격 조회, 재시작 후 과거 매핑 조회, 인덱스 리더 변경 후 페이지 조회를 확인했다.

### 15. Global offset 기반 데이터 Fetch API

상태: 완료 · 의존: 14, 기존 physical fetch 경로

구현 커밋: `feat: fetch records by global sequence offset`

**목표:** global 매핑을 조회하고 실제 원본 레코드를 global 순서로 반환한다.
이 단계의 읽기 격리 기준은 `READ_UNCOMMITTED`이며, `READ_COMMITTED`는 16번에서 완성한다.

- Global 범위, `maxBytes`, deadline 등 데이터 읽기에 필요한 요청/응답 계약을 추가한다.
- 매핑에 포함된 각 source partition의 현재 리더에서 physical 배치를 읽는다.
- Physical 읽기를 제한된 병렬 작업으로 수행하되 결과는 global 순서로 조립한다.
- 앞 구간이 미완료·실패했으면 뒤 구간을 먼저 반환하거나 cursor를 넘기지 않는다.
- 원본 record batch와 physical/global 매핑, 배치 내 선택 범위, `nextGlobalOffset`을 반환한다.
- Physical offset을 덮어써 원본 CRC나 producer 메타데이터를 바꾸지 않는다.
- `maxBytes`는 전달하는 전체 배치 크기에 적용한다. 진행을 위한 첫 배치 초과 허용과
  프로토콜 응답 크기 상한을 구분하며 배치를 바이트 단위로 절단하지 않는다.
- Topic READ와 내부 physical 읽기의 권한, bandwidth/request quota, leader 이동 및 하나의 deadline을 적용한다.
- Retention 등으로 매핑 대상 데이터가 없으면 해당 위치에서 오류를 반환하고 조용히 건너뛰지 않는다.

주요 위치: `clients` 프로토콜, `GlobalSequenceFetchApis`, `GlobalSequenceFetchManager`,
`GlobalSequenceDataRouter`, `GlobalSequenceDataReader`, `BrokerServer`/`KafkaApis`.

완료 기준: 여러 파티션의 레코드를 global offset으로 읽을 수 있고 반복 페이지 읽기의
순서·cursor·선택 범위가 일치한다. 역순 RPC 완료, 앞 구간 실패, 큰 배치, 배치 중간 범위,
데이터 리더 변경·삭제·timeout·권한·quota를 검증한다.

검증: core 540개, clients 48개, global-sequence-coordinator 77개, 총 665개 테스트 통과.
원본 압축별 바이트·CRC, 열린/abort 트랜잭션의 READ_UNCOMMITTED 읽기, 순서·cursor,
리더 이동·재시작·retention·timeout·ACL·quota와 기존 쓰기·복구·조회 회귀를 확인했다.

### 16. Global Fetch의 트랜잭션 격리

상태: 완료 · 의존: 15

구현 커밋: `feat: honor transaction isolation in global sequence fetch`

**목표:** global 순서를 지키면서 `READ_UNCOMMITTED`와 `READ_COMMITTED`를 제공한다.

- Physical LSO와 abort 정보를 global fetch의 매핑·응답 조립에 연결한다.
- `READ_UNCOMMITTED`는 committed index와 data HW를 기준으로 열린 트랜잭션 데이터도 읽을 수 있게 한다.
- `READ_COMMITTED`는 첫 미확정 global 구간에서 멈추고 뒤 파티션의 확정된 배치를 앞질러 반환하지 않는다.
- Commit/abort가 확정되면 같은 cursor에서 재개할 수 있게 한다.
- Abort된 배치는 필터링하되 이미 할당된 global 범위는 소비하고 번호를 재사용하지 않는다.
- Control batch와 batch/page 경계에서도 필터링 결과와 `nextGlobalOffset`을 일치시킨다.
- 인덱스 커밋을 사용자 트랜잭션 커밋으로 취급하지 않는다.

주요 위치: `GlobalSequenceFetch`, `GlobalSequenceDataReader`, `GlobalSequenceDataRouter`,
`GlobalSequenceFetchManager`, `GlobalSequenceFetchApis`, 공개/내부 Fetch v1 프로토콜.

완료 기준: 열린 앞 트랜잭션과 확정된 뒤 파티션 데이터가 있을 때 순서를 지키며 대기한다.
Commit·abort 후 재개, 모두 필터링된 페이지, global 번호의 빈 구간, control batch,
다중 파티션과 리더 변경 조합을 테스트한다. 15번의 일반 읽기 동작도 유지한다.

검증: core 556개, clients 48개, global-sequence-coordinator 77개, 총 681개 회귀 테스트 통과.
LSO·HW 경합과 marker 복제 경계, 같은 producer의 abort 후 commit, segment/page 경계,
빈 필터링 페이지와 cursor, v0 호환성 및 v1 downgrade 거절을 검증했다.
실제 3브로커에서 열린 트랜잭션의 source 리더 이동, commit/abort, 후속 transaction과
source/index 리더 변경 후 두 격리 수준의 global 읽기를 확인했다.

## 남은 단계

17번부터 순서대로 구현하고, 각 단계의 테스트와 설계 문서를 함께 별도 커밋한다.
추가 설정·지표 이름과 필요한 신규 클래스 이름은 해당 단계에서 확정한다.

### 17. 작업량·메모리 제한, backpressure, 상세 지표

상태: 미구현 · 의존: 10~16

예정 커밋: `feat: bound global sequence work and expose progress metrics`

**목표:** 쓰기·복구·global 읽기가 느리거나 요청이 몰릴 때도 자원 사용량과 진행 상태를 통제한다.
공유 worker, Reader byte 제한과 Runtime 기본 지표는 이미 있으며 이 단계에서 제한·관측 범위를 확장한다.

- Produce waiter, 조회·fetch 작업, RPC 및 큐에 필요한 개수·바이트·시간 상한을 정한다.
- Global lookup의 cache/scan과 fetch 조립 버퍼를 제한하고 취소·timeout·종료 시 자원을 해제한다.
- 과부하 시 신규 작업의 수락·재시도 정책과 오류를 명시한다.
- 이미 append된 원본이나 수락된 인덱스 write를 유실시키는 방식으로 backlog를 줄이지 않는다.
- 파티션 간 공정성을 확인하여 느린 한 파티션이 다른 파티션의 진행을 막지 않도록 한다.
- 인덱싱 지연, Produce 대기 수·시간, RPC 재시도, fencing/gap, 복구 시간,
  retention 보류와 global 읽기 지연·오류 지표를 추가한다.
- 지표마다 physical/global/index offset의 단위를 구분하고 metric cardinality를 제한한다.
- 설정 기본값·유효성·종료 순서와 장애 중 자원 정리를 테스트한다.

주요 변경 위치: coordinator 설정·metrics, Indexer/라우터/waiter, global 조회·fetch 컴포넌트.

완료 기준: 느린 index HW·원격 fetch 및 대량 요청 상황에서 큐·메모리가 설정한 경계 안에 머문다.
과부하 해소 후 같은 committed progress에서 진행하며 중복 할당·누락이 없다.
Batch 묶기나 checkpoint 형식 변경은 자동으로 포함하지 않으며, 필요하면 별도 설계·검증 후 결정한다.

### 18. 종합 장애 테스트와 사용·운영 문서

상태: 미구현 · 의존: 1~17

예정 커밋: `test: verify global sequence fault recovery end to end`

**목표:** Produce부터 복구와 global 읽기까지 연결한 전체 계약을 재현 가능한 장애 테스트로 검증한다.

- 기존 S01~S20 단위·통합 테스트의 보장 범위를 확인하고 전체 경로에서 부족한 조합을 보완한다.
- 데이터 커밋 후 인덱싱 전 종료, 인덱스 append 후 HW 지연, 커밋 응답 유실과 재시도를 제어한다.
- Source leader A→B→A, index leader 교체, 브로커 재시작과 오래된 callback 도착을 조합한다.
- Retention/DeleteRecords와 follower 승격, 토픽 삭제·동일 이름 재생성을 검증한다.
- Global fetch의 역순 응답, 앞 구간 오류, 페이지 경계와 트랜잭션 commit/abort를 검증한다.
- 최종 데이터·인덱스를 독립적으로 읽어 physical batch별 단일 매핑,
  파티션별 순서, global 범위 비중첩과 누락 없는 복구를 확인한다.
- Fault injection, latch, 제어 가능한 Future·HW를 사용하여 임의 sleep에 의존하는 테스트를 줄인다.
- 토픽 생성 설정, acks별 응답 의미, 조회/fetch 예제, 재시도·오류 처리,
  주요 지표와 복구 절차를 문서화한다.
- 지원 범위와 제한, 실행 가능한 검증 명령, 프로토콜 버전 및 모든 브로커의 기능 지원 조건을 기록한다.

주요 변경 위치: 실제 브로커 통합·장애 테스트, 관련 테스트 helper, 상세 설계 및 사용·운영 문서.

완료 기준: 데이터와 인덱스가 정상 복제 복구 가능한 장애 조건에서 쓰기·읽기 계약을 유지하고,
재시작 후에도 global 읽기의 결과와 cursor가 일관된다. 미지원·복구 불가능 조건은 명시적인 오류로 드러난다.
이 단계까지 통과하면 요청한 1차 구현 범위가 완료된다.

## 이후 작업을 진행하는 방법

- 다음 구현 대상은 **17번 처리량 제어·지표**다. 18번은 종합 검증·운영 문서다.
- 단계마다 코드·필요한 테스트·설계 갱신을 함께 검증한 뒤 하나의 구현 커밋으로 저장한다.
- 완료 후 이 문서의 상태와 실제 커밋 해시를 갱신한다. 예정 커밋 제목은 최종 구현 범위에 맞게 조정한다.
- 상세 의미나 오류·offset 경계를 변경할 때는 [상세 설계 문서](global-sequence.md)와 함께 수정한다.
