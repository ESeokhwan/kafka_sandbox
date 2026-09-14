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

# Ordered global sequence topic

상태: 1차 구현을 위한 설계 계약. 모듈·설정, 저장 형식, Runtime context/hook,
Coordinator shard의 할당·진행 상태와 브로커 서비스/lifecycle 연결까지 구현했다.
내부 RPC·리더 라우팅·원본 로그 Reader와 파티션별 자동 Indexer까지 구현했다.
장애 복구의 progress 재확인과 source leader 변경을 연결했다.
원본 보존·Produce 대기 연동 및 global 읽기는 후속 구현 단계다.

기준 코드: Kafka 4.1.1, commit `be816b82d2`.

데이터 파티션별 Indexer가 커밋된 원본 로그를 순서대로 읽고, 토픽별
Coordinator가 각 배치에 global offset 범위를 할당한다. 같은 파티션에서는
앞선 배치를 빠뜨리지 않는다. 장애 후 진행 위치는 커밋된 인덱스 로그에서
복원하며, timeout 때문에 같은 배치에 새 범위를 할당하지 않는다.

## 1. 범위와 활성화

1차 구현에는 일반 Produce 연동, 인덱스 저장, 재시도와 장애 복구,
global offset 기반 인덱스 조회 및 데이터 읽기를 포함한다.

- 데이터 토픽 생성 시 `global.sequence.enabled=true`로 활성화한다.
  기본값은 `false`이며, 기존 토픽의 활성화와 활성 토픽의 비활성화는 지원하지 않는다.
- 기존 파티션과 나중에 추가하는 데이터 파티션 모두 처음 생성된 로그 위치인
  physical offset 0부터 인덱싱한다. 새 데이터 파티션은 기존 토픽의 global
  sequence에 합류한다.
- 데이터 토픽은 토픽 설정에 `cleanup.policy=delete`를 명시한다. 이후 브로커 기본값
  변경으로 compaction이 적용되지 않도록 이 override의 삭제도 거절한다. 데이터 compaction과
  설정 변경을 통한 compaction 활성화는 1차 버전에서 거절한다.
- 기존 KafkaProducer를 사용한다. Produce 응답과 일반 Kafka Fetch의 offset은
  physical offset을 유지한다. Global 읽기는 별도 API로 제공한다.
- Transactional produce와 global 읽기의 `READ_UNCOMMITTED` 및
  `READ_COMMITTED`를 지원한다. 인덱스 커밋과 사용자 트랜잭션 커밋은 별개다.
- 일반 Consumer Group의 offset 저장·할당을 global offset으로 바꾸는 기능,
  기존 토픽 이력의 전환, 데이터 compaction, 원격 계층만을 이용한 인덱싱 복구,
  인덱스 이력 GC는 후속 범위다.
- 1차 버전은 모든 관련 브로커가 이 기능을 지원하는 클러스터를 대상으로 한다.
  Mixed-version 활성화나 rolling upgrade의 호환성을 자동으로 가정하지 않는다.
- 복제 완료 데이터가 유지되는 정상 리더 선출을 전제로 한다. Unclean election이나
  모든 복제본의 손실로 이미 커밋된 원본·인덱스가 사라진 경우까지 복원을 보장하지 않는다.
  감지한 데이터 손실을 offset 건너뛰기나 재할당으로 숨기지 않는다.

## 2. 용어와 offset

| 이름 | 의미 |
|---|---|
| Data partition | 사용자가 Produce하는 원본 토픽의 파티션 |
| Physical batch | 데이터 로그에 저장된 하나의 비-control record batch |
| PhysicalBatchId | `(topicId, physicalPartition, physicalBaseOffset)` |
| Global range | 하나의 배치에 할당한 `[globalBaseOffset, globalBaseOffset + recordCount)` |
| Data HW | 해당 데이터 파티션에서 커밋된 로그의 끝 경계 |
| Index HW | 해당 인덱스 파티션에서 커밋된 로그의 끝 경계 |
| Committed progress | 순서대로 인덱싱 완료한 마지막 데이터 배치와 재개 위치 |
| Pending allocation | Replay되었으나 아직 커밋 완료로 확인되지 않은 할당 |
| Source leader epoch | 데이터 파티션 리더의 세대 |
| Indexer generation | 같은 데이터 리더 내 재등록까지 구분하는 인덱서 소유권 세대 |
| Coordinator leader epoch | 인덱스 파티션 리더의 세대 |

HW와 모든 end offset은 exclusive 경계다. `HW=100`이면 로그 offset 99까지
커밋된 것이며, offset 100은 포함하지 않는다. 인덱스 로그의 offset은 데이터의
physical offset 및 global offset과 다른 좌표다. Timeline snapshot 조회에 사용하는
offset 기반 epoch도 리더 세대 번호와 구분한다.

PhysicalBatchId에는 리더 epoch를 넣지 않는다. 같은 커밋된 배치를 새 리더가
재시도해도 배치 식별자가 같아야 한다. 전달자가 현재 소유자인지는 별도로 검사한다.

예를 들어 physical offset 100~102의 레코드 세 개에 global range `[7, 10)`을
할당하면, 각 레코드는 global offset 7, 8, 9에 대응한다. 1차 버전의 원본 배치는
compaction되지 않으므로 배치 내 대응은 다음과 같다.

```text
globalOffset = globalBaseOffset + (physicalOffset - physicalBaseOffset)
resumeOffset = physicalLastOffset + 1
```

`physicalLastOffset`은 실제 배치에서 읽는다. 배치 사이의 다음 위치를
`recordCount`만으로 추정하지 않는다. Control batch 등은 별도로 순회해야 한다.

## 3. 정확성 계약

### I1. 토픽별 global 순서

Global sequence는 topic ID별로 0에서 시작한다. 서로 다른 데이터 파티션의
할당 요청은 같은 토픽을 담당하는 coordinator shard에서 직렬로 처리한다.
신규 배치는 당시의 다음 global offset부터 record count만큼 연속된 범위를 받는다.
커밋된 global 범위는 겹치지 않으며, 이미 커밋된 번호를 재사용하지 않는다.

복구 중 로그에서 제거된 미커밋 할당은 성공한 할당이 아니다. 그 speculative 상태는
로그와 함께 되돌릴 수 있다. 커밋된 prefix와 speculative 상태를 혼동해서는 안 된다.

### I2. 데이터 파티션별 순서와 누락 방지

설정된 시작 위치 이후 같은 파티션의 대상 배치가 A, B, C 순서라면 C의 인덱스
커밋은 A와 B의 인덱스 커밋을 전제로 한다. 단순한 최대 관측 offset을 완료 경계로
사용하지 않는다. 이 prefix 보장을 유지한 상태에서만 진행 위치로 중복을 판단한다.

다른 파티션의 배치는 사이에 들어갈 수 있다. P0가 A→B→C이고 P1이 D→E라면
A→D→B→E→C는 유효하지만 B→A→D→E→C는 유효하지 않다. 레코드 timestamp나
Producer의 호출 시간을 기준으로 파티션 간 순서를 정하지 않는다.

### I3. 안정된 원본에 대한 할당

Indexer는 현재 데이터 리더의 로그에서 `physicalLastOffset < dataHW`인 완전한
배치만 제출한다. Control batch에는 global 번호를 할당하지 않는다. 열린
트랜잭션의 데이터 배치는 data HW 아래에 있으면 인덱싱 대상이다.

### I4. 원자적인 인덱스 상태 변경

신규 배치의 인덱스 레코드와 그 결과의 `nextGlobalOffset` 메타데이터를 하나의
atomic coordinator 결과로 기록한다. 인덱스 HW가 전체 write의 끝을 커버해야
성공을 알린다. 로컬 append, 메모리 replay, Future 생성만으로 완료를 선언하지 않는다.

### I5. 재시도와 timeout

같은 PhysicalBatchId의 재시도는 새 global 범위를 생성하지 않는다. RPC timeout과
Producer timeout은 write 취소나 rollback의 근거가 아니다. 실제 로그 rollback 또는
리더 교체 후 확인된 로그 상태만 speculative 할당을 제거할 근거가 된다.

### I6. 요청과 인덱싱의 수명 분리

인덱싱은 데이터 로그를 따라 진행한다. 원래 Produce 요청이 종료되어도 대상 데이터가
남아 있으면 계속 처리한다. 메모리에만 있는 작업 큐를 복구의 유일한 근거로 사용하지 않는다.

### I7. 보장의 단위

중복 방지는 물리 배치의 인덱스 할당에 대한 보장이다. 비멱등 Producer가 같은 내용을
새 physical offset에 다시 저장하면 다른 배치로 인덱싱한다. 메시지 내용에 대한 중복
제거나 애플리케이션의 end-to-end exactly-once를 제공한다는 의미는 아니다.

## 4. 컴포넌트와 실행 모델

```text
KafkaApis: Produce
  -> 데이터 로그 append
  -> 해당 physical batch의 인덱싱 완료 대기

데이터 파티션의 HW 알림 / 복구 후 시작
  -> 파티션별 Indexer
  -> 원본 로그에서 다음 대상 배치 읽기
  -> IndexRoutingManager
  -> 로컬 또는 원격 GlobalSequenceCoordinator
  -> 인덱스 로그 append와 복제
  -> 인덱스 HW 갱신
  -> Indexer 및 Produce waiter 완료
```

| 컴포넌트 | 책임 |
|---|---|
| KafkaApis | 일반 Produce 검증·append를 유지하고 인덱싱 완료를 비동기적으로 기다림 |
| Partition Indexer | 원본 로그의 순차 읽기, 파티션별 한 논리적 append 진행, 재시도 |
| IndexRoutingManager | 인덱스 리더 탐색, 내부 RPC, 연결 오류와 metadata 변경 처리 |
| GlobalSequenceCoordinator | 소유권·순서·중복 확인, 범위 할당, 진행 위치 제공 |
| CoordinatorRuntime | 이벤트 직렬화, replay, 저장, snapshot, HW와 완료 Future 관리 |
| Global reader | 커밋된 인덱스 조회, physical fetch, global 순서와 격리 수준 적용 |

Indexer는 데이터 파티션 리더에서 실행한다. 파티션마다 전용 스레드를 만들 필요는
없으며 executor를 공유한다. 파티션 알림 callback에서는 작업 예약만 하고, lock을
보유한 상태에서 로그 scan, RPC 또는 Future 대기를 하지 않는다.

같은 파티션에는 최초 구현에서 하나의 논리적 append만 진행한다. Timeout 후 같은
배치를 재전송하는 것은 새로운 논리적 append가 아니다. 다음 배치는 앞선 append가
커밋됐거나 이미 커밋된 것으로 확인된 뒤에 진행한다. 다른 파티션의 작업은 독립적이다.
이후 연속 배치를 한 요청으로 묶는 최적화도 이 규칙을 유지해야 한다.

### 리더 라우팅 구현

브로커의 `IndexRoutingManager`는 data topic UUID를 서비스의 고정 partition 함수에
전달하고, 현재 KRaft metadata image에서 해당 인덱스 파티션의 리더와 inter-broker
endpoint를 찾는다. 리더가 로컬이면 coordinator Future를 직접 연결하고, 원격이면
`GlobalSequenceNetworkClient`의 `InterBrokerSendThread`를 통해 v0 내부 RPC를 보낸다.
원격 연결은 브로커의 inter-broker listener·SSL/SASL 설정과 ApiVersions 협상을 재사용한다.
프로토콜 미지원·인증 실패는 성공으로 간주하거나 이전 프로토콜로 우회하지 않는다.

각 시도는 index topic UUID, partition, leader broker/epoch, broker registration epoch,
endpoint를 캡처한다. 성공 응답 시 현재 metadata 및 응답의 coordinator epoch를 다시
검사한다. 리더가 바뀐 뒤 도착한 성공·조회 결과는 폐기하고 같은 논리적 요청을 재시도한다.
결과에 포함된 `CoordinatorLocation`과 `isCurrent`는 비동기 복구 작업이 실제 적용되는
시점에도 이 검사를 수행하기 위한 수단이다. Write의 source broker/epoch는 송신 전과
성공 응답 시 재확인하며, 실제 coordinator도 write 실행 시 별도로 검증한다.

연결 단절, timeout, coordinator 로딩/이동, metadata 미도착과 일시적인 복제 오류는
100ms부터 최대 1초까지 backoff하여 재시도한다. KRaft가 image를 갱신하므로 별도의
controller RPC나 고정된 리더 캐시를 쓰지 않는다. 요청 전체에는 호출자가 지정한
하나의 deadline을 적용하며, 재시도할 때 예산을 새로 시작하지 않는다. 등록 UUID와
expected generation, physical batch 및 predecessor, source identity는 그대로 유지하고
대상 coordinator epoch만 새 리더에 맞춘다.

등록 CAS 거절과 `FENCED`, `OWNER_NOT_COMMITTED`, `OUT_OF_ORDER`는 호출자에게
반환한다. 특히 fencing/등록 거절은 인덱스 리더가 바뀌었더라도 자동 재시도로 숨기지 않는다.
Indexer는 이 결과에 맞춰 중단하거나 같은 등록 barrier를 재확인한 뒤 진행 위치를 조회한다.
`OUT_OF_ORDER`도 같은 등록 barrier와 committed progress를 재확인한다. 동일 리더에서
재확인한 뒤에도 같은 순서 오류가 반복되면 cursor를 건너뛰지 않고 중단한다.
Deadline·호출 취소·브로커 종료는 대기 Future와 예약된 재시도를 해제하며, 이미 coordinator가
수락한 write의 rollback이나 할당 취소를 의미하지 않는다. Broker 종료 시 라우터와 네트워크를
coordinator 및 scheduler보다 먼저 닫는다. 관측한 인덱스 토픽의 삭제·UUID/partition 수 변경은
새 이력으로 재시도하지 않고 오류로 중단한다.

인덱스 리더 이동 직후에는 새 리더의 로그에 batch가 존재해도 HW 전파가 아직 끝나지
않을 수 있다. 이때 Describe는 현 시점에서 확인된 committed progress만 반환하며,
이전 응답보다 뒤처져 보일 수 있다. 이를 복구의 시작 위치로 바로 적용하지 않는다.
같은 registration UUID·조건으로 등록 barrier를 다시 확인한 후 progress를 조회한다.
3개 브로커 통합 테스트는 원격 등록/append, 인덱스 리더 이동, 같은 등록의 재확인,
중복 append의 `ALREADY_INDEXED`, 다음 배치의 연속 global offset 할당을 검증한다.

### 파티션별 자동 Indexer 구현

[GlobalSequenceIndexerManager](../../core/src/main/scala/kafka/server/GlobalSequenceIndexerManager.scala)는
활성화된 데이터 토픽의 로컬 리더 파티션마다
[GlobalSequencePartitionIndexer](../../core/src/main/scala/kafka/server/GlobalSequencePartitionIndexer.scala)를 만든다.
`BrokerMetadataPublisher`가 ReplicaManager의 replica 변경과 동적 로그 설정 적용을 마친 뒤
manager에 metadata를 전달한다. Topic UUID·실제 Partition 인스턴스·source leader epoch로
실행 수명을 구분한다. 같은 수명에서 실패하거나 fenced된 Indexer는 무관한 metadata
갱신만으로 재시작하지 않는다. source epoch 변경은 이전 인스턴스를 닫고 새 인스턴스를 만든다.

각 Indexer는 다음 순서로 진행한다.

1. Describe로 현재 generation을 확인하고, UUID를 한 번 생성하여 CAS 등록을 요청한다.
   이 최초 Describe의 progress는 원본 읽기 위치로 사용하지 않는다.
2. 등록 Future가 완료된 뒤 같은 인덱스 리더에서 committed progress를 다시 조회한다.
   Progress가 없으면 physical 0, 있으면 마지막 배치의 `lastOffset + 1`에서 읽는다.
3. Reader가 반환한 완전한 데이터 배치를 직전 인덱싱 배치의 base offset과 함께 append한다.
   등록 identity·물리 배치·predecessor·captured data HW를 하나의 immutable 요청으로 유지한다.
4. `INDEXED` 또는 `ALREADY_INDEXED` Future가 완료되어 committed prefix를 확인한 뒤에만
   다음 데이터 배치를 읽는다. Control-only `CONTINUE` 결과는 global 번호를 할당하지 않고
   읽기 위치만 전진시킨다. `AWAIT_HIGH_WATERMARK`이면 다음 HW 알림까지 대기한다.

파티션마다 별도 스레드를 만들지 않는다. 브로커의 고정 worker pool에 파티션별 직렬 작업을
예약하고, 한 차례 실행에 원본 읽기를 최대 한 번 수행하여 다른 파티션에도 실행 기회를 준다.
RPC Future를 기다리는 동안 worker를 점유하지 않는다. HW listener는 최대 한 개의 wakeup을
예약하며 로그를 읽거나 RPC를 보내지 않는다. 읽는 동안 HW가 증가했다면 알림 HW와 Reader가
캡처한 HW를 비교하여 다시 읽는다. 배치 중간 HW에서 `cursor < HW`라는 이유로 반복 실행하지 않는다.

| 브로커 설정 | 기본값 | 의미 |
|---|---|---|
| `global.sequence.indexer.num.threads` | 2 | 원본 읽기와 Indexer 작업을 수행하는 공유 worker 수 |
| `global.sequence.indexer.read.max.bytes` | 1048576 | 한 번의 원본 읽기 byte soft limit. 첫 완전한 배치는 초과 가능 |

두 설정은 양수인 정적 설정이다. 각 라우팅 호출의 deadline에는
`global.sequence.coordinator.write.timeout.ms`를 사용한다. 라우터가 retryable 오류로
호출을 끝내면 브로커 scheduler에서 100ms 뒤 동일 요청을 재전송한다. 등록 timeout은
새 UUID/generation을 만들지 않으며 append timeout도 다음 배치로 넘어가지 않는다.
인덱스 리더가 바뀌어 queued 응답의 route token이 만료되었거나 `OWNER_NOT_COMMITTED`를
받으면 같은 등록 요청으로 barrier를 재확인하고 committed progress를 조회한다.
`FENCED` 및 CAS 거절은 중단 조건이며 새 generation을 획득하는 재시도로 바꾸지 않는다.

Follower 전환·partition 실패·삭제는 인스턴스를 즉시 중단 상태로 만들고 listener와
대기 중인 라우팅 Future, 예약한 재시도를 해제한다. 이미 큐에 있거나 실행 중인 읽기/RPC의
늦은 결과로 다음 append를 시작하지 않는다. 브로커 종료는 Indexer와 worker를 먼저 닫고
그 뒤 라우터·scheduler·coordinator·ReplicaManager를 닫는다. 라우팅 Future 취소는 이미
coordinator가 수락한 write를 rollback하지 않는다.

단위 테스트는 한 논리적 append 제한, 등록/append timeout의 동일 요청 재사용, HW 알림
경합, control-only 전진, route token 만료와 ownership 중단, 공유 worker 및 metadata
lifecycle을 검증한다. 통합 테스트는 별도 수동 append 없이 Produce된 두 파티션의 데이터가
인덱싱되는지, 인덱스 리더 이동 후에도 실제 index log의 global 범위가 중복 없이 연속적인지,
브로커 재시작 후 committed progress에서 자동 인덱싱이 재개되는지 확인한다.

Source gap과 predecessor 불일치는 같은 등록 barrier 이후 authoritative progress를 재확인한다.
재확인 후에도 복구할 수 없으면 읽기 위치를 건너뛰지 않고 오류로 중단한다.
Retention/DeleteRecords 보존 경계는 12번 단계다. Produce 응답은 아직 인덱스 커밋을 기다리지 않으며, 해당 대기는
13번에서 연결한다. Global 조회/Fetch API도 후속 단계다.

### 원본 로그 Reader 구현

`GlobalSequenceSourceReader.read`는 topic UUID·데이터 파티션·source leader epoch,
재개할 physical offset과 byte 제한을 받는다. 로컬 데이터 리더의 실제 로그 UUID와
활성화 설정을 확인하고, 현재 epoch를 지정해 Partition의 fetch 경로로 읽는다.
각 호출은 동기 I/O이므로 Indexer worker에서 실행하며, HW/leadership callback이나
파티션 lock을 잡은 상태에서 호출하지 않는다. 로그 파일 slice는 Reader 내부에서만
사용하고 결과에는 immutable `PhysicalBatch`와 캡처한 data HW·epoch·재개 위치만 담는다.

HW를 파일 위치로 materialize하는 `fetchOffsetSnapshot` 대신 0바이트 `LOG_END` fetch로
읽기 경계를 캡처한다. 이 방식은 배치 중간의 HW를 배치 시작 위치로 조정하지 않는다.
본문도 `LOG_END`로 읽되 `physicalLastOffset < capturedHW`인 완전한 배치만 반환한다.
읽는 동안 HW가 더 증가해도 이번 호출의 경계를 확대하지 않는다. 배치 중간의 HW는
로그 누락으로 취급하지 않고 해당 배치의 base offset에서 HW 갱신을 기다린다.

| 결과 | 호출자의 다음 동작 |
|---|---|
| `BATCH` | 반환된 데이터 배치를 인덱싱한다. 커밋/AlreadyIndexed 확인 후에만 `nextPhysicalOffset`을 소비한다. |
| `CONTINUE` | 읽기 제한 또는 segment 경계까지 control batch만 검증했다. `nextPhysicalOffset`부터 다음 읽기를 예약한다. |
| `AWAIT_HIGH_WATERMARK` | 현재 HW 아래에 더 읽을 완전한 배치가 없다. 다음 HW 알림을 기다리고 읽기 경합으로 알림을 놓치지 않도록 재확인한다. |

한 호출은 데이터 배치 하나까지만 반환한다. Control-only 구간도 byte 제한 안에서
순회하므로 긴 구간이 다른 파티션의 작업을 독점하지 않는다. 첫 배치 하나는 진행을 위해
byte 제한을 초과할 수 있다. Slice 끝에서 잘린 뒤 배치는 건너뛰지 않고 다음 호출에서
완전히 읽는다. 압축 배치는 CRC와 header의 범위·count, 내부 레코드의 연속 offset을
검증하며, key/value를 보관하지 않는 streaming iterator를 사용한다. Kafka 4.x의 새
로그에 기록되는 format v2를 지원한다. Control batch에는 번호를 할당하지 않고 열린
트랜잭션과 abort된 트랜잭션의 데이터 배치는 모두 인덱싱 대상으로 반환한다.

반환 직전 Partition·로그 객체·실제 topic UUID·leader epoch와 읽기 경계를 다시 확인한다.
리더나 로그가 교체되면 결과를 버리고, 같은 epoch에서 HW가 후퇴하거나 필요한 원본이
삭제되면 실패한다. 재개 위치가 local log start보다 앞선 경우, HW 아래에서 빈 읽기가
발생한 경우, 필요한 batch base offset이 빠진 경우에는 `SourceLogGapException`을
반환한다. Remote tier만 남은 범위를 자동으로 건너뛰지 않는다. 이 오류를 받은 복구
흐름은 coordinator의 권위 있는 progress를 재확인한 뒤 실제 손실 여부를 판단해야 한다.
Reader 자체는 cursor를 영속화하거나 인덱스를 할당하지 않는다.

## 5. 저장과 메모리 상태

내부 토픽은 `__global_sequence_index`다. 데이터 topic ID를 결정적인 함수로 매핑하여
하나의 인덱스 파티션에 배정한다. 같은 토픽의 모든 데이터 파티션은 이 shard를 사용한다.
데이터 파티션 수는 늘릴 수 있지만 인덱스 파티션 수의 변경으로 기존 토픽의 배정이
바뀌어서는 안 된다. 1차 버전에서는 내부 인덱스 토픽의 파티션 수 변경을 지원하지 않는다.

저장 레코드의 의미는 다음과 같다.

| 레코드 | Key | Value의 주요 내용 |
|---|---|---|
| BatchIndex | topic ID, global base offset | physical partition, base/last offset, record count |
| TopicMetadata | topic ID | next global offset |
| IndexerFence | topic ID, physical partition | source leader epoch, indexer generation 및 등록 식별 정보 |

각 파티션의 committed progress는 커밋된 BatchIndex 이력에서 복원한다. 별도의
영속 진행 커서를 초기 설계의 필수 조건으로 두지 않는다. Source 소유권을 보존하는
IndexerFence와 global 번호를 보존하는 TopicMetadata는 진행 커서와 역할이 다르다.

Coordinator 메모리에는 토픽별 committed/speculative 다음 global offset, 파티션별
committed progress, 미커밋 할당 및 소유권을 구분해 보관한다. 재로딩할 때도 committed
prefix와 남은 tail을 구분한다. 진행 상태의 크기는 전체 인덱스 이력이 아니라 토픽·파티션
수와 제한된 pending 작업 수를 기준으로 제한한다.

과거 global 매핑용 cache는 선택적인 조회 최적화다. Cache eviction이 신규 할당 여부를
바꾸어서는 안 된다. 전체 과거 배치 ID를 무제한 메모리 맵에 보관하는 방식에 의존하지 않는다.

### 저장 형식 v0

스키마는 [모듈의 JSON 정의](../../global-sequence-coordinator/src/main/resources/common/message)에
저장하며 Kafka MessageGenerator로 Java 메시지, JSON converter와 record type enum을 생성한다.
아래 type ID는 내부 인덱스 로그의 record type이며 네트워크 RPC의 API key와 별개다.

| Type ID | Key 필드 순서 | Value v0 필드 순서 |
|---|---|---|
| 0: BatchIndex | TopicId: uuid, GlobalBaseOffset: int64 | PhysicalPartition: int32, PhysicalBaseOffset: int64, PhysicalLastOffset: int64, RecordCount: int32 |
| 1: TopicMetadata | TopicId: uuid | NextGlobalOffset: int64 |
| 2: IndexerFence | TopicId: uuid, PhysicalPartition: int32 | SourceBrokerId: int32, SourceLeaderEpoch: int32, IndexerGeneration: int64, RegistrationId: uuid |

공통 CoordinatorRecordSerde 형식에 따라 key는 int16 type ID로 시작하고, value는 int16
version으로 시작한다. Key는 비-flexible 고정 형식, value v0는 flexible 형식이다.
배치 key에 global base offset을 포함하므로 서로 다른 배치가 compaction key를 공유하지 않는다.
RegistrationId는 재시도하는 등록 작업의 UUID이며 topic ID 및 등록 UUID의 zero 값은 사용하지 않는다.
Coordinator leader epoch는 현재 리더의 실행 context이므로 영속 배치 식별자에 넣지 않는다.

직렬화 계층은 null value를 tombstone으로 표현한다. 이는 인덱스 GC를 허용한다는 뜻이 아니며,
어떤 tombstone을 수락·적용할지는 shard의 lifecycle 계약에서 검증한다. 알 수 없는 type이나
value version 및 잘린 레코드는 로딩 오류로 처리한다. Flexible value의 알 수 없는 tagged field는
보존한다. v0의 type ID·필드 순서·바이트 배치는 고정 fixture 테스트로 검증하며, 이후 형식 변경은
명시적인 버전 호환성 작업을 거친다.

레코드 생성 helper는 UUID, 음수, 배치 개수·범위 일치 및 end offset overflow를 검사한다.
원본 순서, 현재 소유권, 중복 및 replay한 상태의 유효성은 coordinator shard가 별도로 검증한다.
하나의 신규 할당에 대한 BatchIndex와 TopicMetadata를 atomic write로 묶는 책임도 shard에 있다.

### Runtime context와 상태 알림

`CoordinatorRuntime.scheduleWriteOperationWithContext`는 `(shard, context)` callback에
`CoordinatorWriteContext(highWatermark, leaderEpoch)`를 전달한다. 값은 요청을 큐에 넣을 때가
아니라 active coordinator의 lock을 잡고 write 이벤트를 실행할 때 캡처한다. 기존의
`scheduleWriteOperation`은 그대로 사용할 수 있다.

여기서 highWatermark는 Runtime이 적용한 커밋 경계다. 원본 파티션의 HW가 아니며,
인덱스 파티션의 실제 HW 알림이 큐에 대기 중이면 그보다 늦을 수 있다. Shard의 committed/
pending 상태와 함께 사용해야 한다. Context는 immutable한 관측값이므로 callback 종료 후의
리더 소유권까지 보장하지 않는다. 비동기 scan 결과를 반영할 때는 다음 write 이벤트에서
epoch와 현재 진행 상태를 다시 검사한다. Write Future가 나중에 완료되어도 처음 전달한
context가 자동으로 최신 값으로 바뀌지는 않는다.

공통 shard에는 기존 구현에 영향을 주지 않는 기본 no-op hook 두 개가 있다.

- `onHighWatermarkUpdated(hw)`: 커밋 경계가 증가할 때, 이전 snapshot 삭제 및 write Future
  성공 처리 전에 호출한다. Loader의 replay 중 `onLoaded`보다 먼저 호출될 수도 있다.
  현재 메모리에는 미커밋 tail도 있으므로 HW 기준 snapshot이나 로그 위치로 committed 상태를
  구분한다. 동일 HW의 반복 알림은 hook을 다시 호출하지 않는다.
- `onRollback(offset)`: snapshot을 복원한 직후, 해당 write 실패 응답 전에 호출한다.
  아직 로컬 append되지 않은 replay를 취소할 때는 offset이 기존 lastWrittenOffset과 같을
  수 있다. Timeout만으로 호출하지 않으며, HW보다 뒤로 되돌리는 요청은 거절한다.

두 hook은 다른 shard callback과 직렬로 실행되며 blocking IO를 수행하지 않는다. Hook이
실패하면 해당 shard의 추가 실행을 막고 재로딩이 필요한 실패 상태로 전환한다. 이전 shard에
등록했던 HW listener에서 늦게 도착한 알림은 새 shard의 커밋이나 Future 완료에 적용하지 않는다.
빈 레코드를 반환하는 write도 앞선 buffer 또는 로컬 로그의 pending write가 커밋될 때까지
기다린다. 이 경로는 timeout된 물리 배치 재시도에 새로운 대기를 연결할 때 사용한다.

### 할당과 진행 상태 구현

[GlobalSequenceCoordinatorShard](../../global-sequence-coordinator/src/main/java/org/apache/kafka/coordinator/globalsequence/GlobalSequenceCoordinatorShard.java)의
`prepareAppend(request, context)`를 Runtime의 write callback 안에서 호출한다. 요청에는
물리 배치, 직전 대상 배치의 base offset(최초에는 -1), 원본 data HW 및 Indexer identity를
담는다. Identity는 source broker/leader epoch, generation, registration UUID로 구성한다.
요청 범위 검증과 실제 원본 로그·metadata 검증은 별개다. 서비스는 실행 시점 metadata의
대상 토픽과 source leader/epoch를 검증한다. 원본 로그 순회와 네트워크 권한 검사는
순차 Reader/Indexer와 내부 RPC에서 각각 수행한다.

`prepareAppend`는 상태를 직접 변경하지 않는다. 신규 배치에는 `BatchIndex`와
`TopicMetadata`를 atomic `CoordinatorResult`로 반환하며, Runtime이 이를 replay할 때
speculative sequence와 진행 상태가 바뀐다. IndexerFence가 없거나 identity가 다르면
`FENCED`, 현재 소유권 레코드가 아직 커밋되지 않았으면 `OWNER_NOT_COMMITTED`다.
등록 API는 metadata 검증과 조건부 generation 변경을 수행하며, 동일 registration identity로 재시도한다.

상태의 보관 단위와 갱신 시점은 다음과 같다.

| 상태 | 보관 범위 | 갱신 시점 |
|---|---|---|
| Speculative nextGlobalOffset·최신 배치·소유권 | 토픽/파티션별 최신 값, Timeline snapshot 포함 | 완전한 할당 쌍 또는 fence replay |
| Committed nextGlobalOffset·최신 배치·소유권 | 토픽/파티션별 최신 값 | 해당 기록의 exclusive end까지 index HW 도달 |
| Pending 할당·fence | 아직 커밋되지 않은 로그 tail | replay 시 추가, HW 반영 또는 실제 rollback 시 제거 |

각 할당의 end는 `TopicMetadata` 다음 인덱스 로그 offset이다. HW callback은 이 end로
정렬한 pending 기록만 순회해 커밋 상태를 전진시킨다. 과거 모든 매핑이나 모든 데이터
파티션을 HW마다 scan하지 않는다. Rollback 시 Timeline이 speculative 상태를 먼저
복원하고, shard hook이 남지 않은 tail의 pending 항목을 제거한다. Committed 상태는
되돌리지 않으므로, snapshot 생성 후 커밋된 항목이 rollback 때문에 pending으로
되살아나지 않는다.

Replay는 BatchIndex 다음에 대응하는 TopicMetadata가 오는지, global 범위가 연속인지,
같은 파티션의 physical 범위가 전진하는지 검사한다. 로딩 중에는 HW 통지가 늦어 같은
파티션의 여러 배치가 pending으로 보일 수 있으므로 이 tail도 보관한다. `onLoaded`는
불완전한 할당 쌍을 거절한다. V0에서는 인덱스 offset 0부터 연속된 이력이 필요하며,
tombstone과 인덱스 로그 자체의 transaction은 거절한다. 원본 데이터의 transaction은
이 제한과 별개다.

`AppendResponse.indexedThrough`는 Runtime Future가 성공한 시점에 보장되는 물리 prefix다.
신규/pending 요청의 `INDEXED` 응답을 Future 완료 전에 전송해서는 안 된다. Pending
재시도는 기록을 추가하지 않고 같은 global base를 반환하며 Runtime에서 새로 기다린다.
`ALREADY_INDEXED`는 현재 committed progress를 반환하고 과거 global base는 생략한다.
빈 결과도 Runtime의 기존 대기 규칙을 따르므로 다른 선행 write 때문에 완료가 늦어질
수 있다. 현재 committed progress 조회는 직렬화된 shard 호출 안에서 수행한다.

### 브로커 서비스와 내부 토픽 lifecycle

[GlobalSequenceCoordinatorService](../../global-sequence-coordinator/src/main/java/org/apache/kafka/coordinator/globalsequence/GlobalSequenceCoordinatorService.java)는
`CoordinatorRuntime`을 소유한다. 브로커는 `CoordinatorPartitionWriter`와 전체 이력 검증을
켠 `CoordinatorLoaderImpl`을 주입한다. 최초 metadata publish에서 LogManager와
ReplicaManager 다음에 서비스를 시작하고, 인덱스 파티션 리더 선출/사임에 따라 shard를
load/unload한다. Metadata는 load를 예약하기 전에 서비스와 Runtime에 전달한다.
브로커 종료 시에는 ReplicaManager보다 먼저 서비스를 닫는다. 서비스 시작 전 브로커
startup이 실패했을 때도 생성한 loader·timer·event processor·executor·Runtime metrics를
해제한다. 이미 종료한 서비스는 재사용하지 않으며 브로커 재시작은 새 인스턴스를 만든다.

Java 서비스의 `appendIndex`와 `committedProgress`는 비동기 Runtime 작업을 반환한다.
진행 조회는 committed 상태만 읽고, append는 실행 시점의 source broker/leader epoch도
확인한다. 내부 네트워크 RPC와 파티션별 Indexer는 이 비동기 API를 사용한다.

데이터 topic ID의 shard는 `Utils.abs(topicId.hashCode()) % N`으로 정한다. 이 Kafka Uuid의
hash는 UUID 상·하위 64비트의 XOR를 다시 상·하위 32비트 XOR로 접은 값이다. N은 정적
`global.sequence.coordinator.index.topic.num.partitions`다. 시작할 때 이미 존재하는
내부 토픽의 파티션 수가 설정과 다르면 시작을 거절한다. Controller도 내부 인덱스
토픽의 CreatePartitions를 거절한다. 관련 브로커는 같은 N을 설정해야 한다.

활성 데이터 토픽이 있고 인덱스 토픽이 없으면 `AutoTopicCreationManager`를 통해
브로커 권한의 생성 요청을 보낸다. 일반 `auto.create.topics.enable` 설정과 독립적이다.
생성 요청의 중복은 기존 inflight 관리로 억제하며, metadata에서 토픽을 확인할 때까지
1초 간격으로 재시도한다. 따라서 replication factor보다 브로커가 적거나 controller가
잠시 없을 때도 이후 재시도할 수 있다. 활성 토픽이 없어지거나 서비스가 종료되면
재시도를 취소한다. 한번 관측한 인덱스 토픽이 삭제되거나 다른 UUID로 교체되면
기존 shard를 unload하고 오류로 중단하며 빈 토픽을 자동 재생성하지 않는다.

내부 토픽에는 `cleanup.policy=delete`, `retention.ms=-1`, `retention.bytes=-1`,
`unclean.leader.election.enable=false`를 명시한다. Controller는 이 override의 삭제나
다른 값으로 변경하는 요청도 거절하며 서비스는 metadata에서 이를 재확인한다.
Shard별 할당/Indexer 지표는 후속 단계이고, 현재는 `global-sequence-coordinator-metrics`
그룹으로 Runtime 상태·로딩·이벤트 큐·flush 지표를 수집한다.

인덱스 loader는 offset 0 이전 이력 손실, 누락된 레코드, 알 수 없는 record type,
control batch 및 log end까지 읽지 못한 경우를 실패로 처리한다. 기존 Group/Share
loader의 기본 동작은 유지한다. 공통 Runtime은 완료된 load의 shard identity도 확인해
사임 이전의 늦은 성공/실패가 새로 로딩 중인 shard를 활성화하거나 실패시키지 못하게 한다.

## 6. 소유권 등록과 fencing

데이터 리더의 Indexer는 append 전 소유권을 등록한다. Coordinator는 source leader
identity와 epoch를 metadata에 대조하고, 더 오래된 epoch나 이미 대체된 generation을
거절한다. Metadata 반영 지연으로 현재 소유자를 확인할 수 없으면 성공을 추정하지 않고
재시도시킨다. 내부 호출에는 `CLUSTER_ACTION` 권한이 필요하다.

등록 자체도 재시도 가능해야 한다. 동일한 등록 작업은 동일한 registration identity를
사용하며, 응답 유실로 generation을 반복 증가시키거나 오래된 등록 요청이 새 소유자를
다시 밀어내게 해서는 안 된다. Generation 획득에는 이전 generation과의 조건부 변경을
사용하는 등 이 계약을 만족시키는 프로토콜을 구현한다. Fenced Indexer는 작업을 중단하며
옛 요청으로 소유권을 자동 재탈취하지 않는다.

새 소유권의 fence 기록은 coordinator에서 먼저 수락된 write 뒤에 직렬로 기록한다.
등록 완료는 이 fence가 커밋되어 선행 pending write가 커밋되었거나, 복구 과정에서
그 write의 제거가 확인된 뒤에만 알린다. 이후 committed progress를 읽어 새 Indexer를
시작한다. 등록 timeout도 이 barrier를 우회할 근거가 아니다.

Coordinator leader epoch는 이 소유권과 별개다. 복구·조회 작업은 인덱스 리더 epoch와
읽기 경계를 캡처하며, 리더가 바뀐 뒤 완료된 오래된 결과를 검증 없이 적용하지 않는다.

### 내부 RPC v0 구현

세 API는 broker listener에서 제공하며 `CLUSTER_ACTION` 권한을 먼저 검사한다.
승인된 내부 요청은 request quota에서 제외하고 권한 실패는 throttle 대상이다.
이 브랜치의 API 번호는 93–95이며 upstream Kafka에 예약된 번호가 아니다.
Global sequence 사용 클러스터는 이 프로토콜을 지원하는 브로커로 구성해야 한다.

| API | 번호 | 요청과 결과 |
|---|---|---|
| RegisterGlobalSequenceIndexer | 93 | 토픽 UUID·파티션, source broker/epoch, expected generation, registration UUID → 등록 여부·현재 소유권·coordinator epoch |
| DescribeGlobalSequencePartition | 94 | 토픽 UUID·파티션 → committed physical progress와 최신 소유권 |
| AppendGlobalSequenceIndex | 95 | physical 배치·직전 배치·data HW·소유권 → append status·배치 식별 정보·보장된 progress·coordinator epoch |

등록은 최초 `expectedGeneration=-1`, 이후 관측한 generation을 조건으로 사용한다.
성공 시 generation을 1 증가시킨 fence를 기록한다. 같은 UUID·source·expected generation의
재시도는 같은 fence의 커밋을 기다린다. 조건 불일치는 `Registered=false`와 현재 소유권을
반환하며, 이 응답을 받은 이전 Indexer가 자동으로 새 generation을 획득해서는 안 된다.
등록 timeout은 fence 삭제나 rollback을 뜻하지 않는다.

Describe의 progress는 HW 아래에서 커밋된 배치만 포함한다. 소유권 필드는 등록 CAS에
필요하므로 아직 커밋되지 않은 최신 fence도 포함한다. 조회된 소유권만으로 append를
시작하지 않고, 자신의 등록 성공을 기다린 뒤 progress를 다시 읽는다.

Write 요청의 `CoordinatorLeaderEpoch`가 0 이상이면 write 이벤트 실행 시 실제 epoch와
일치해야 한다. 불일치는 `NOT_COORDINATOR`다. `-1`은 이 검사를 생략하는 내부 API 호환값이며
라우터는 자신이 선택한 리더 epoch를 전달한다. Source broker/epoch도 실행 시 최신 데이터
metadata에 대조하고 불일치는 `FENCED_LEADER_EPOCH`로 반환한다.

Append status는 `INDEXED=0`, `ALREADY_INDEXED=1`, `FENCED=2`,
`OWNER_NOT_COMMITTED=3`, `OUT_OF_ORDER=4`다. 실행 오류는 별도 `ErrorCode`로 전달한다.
미할당 global base offset 및 없는 physical progress는 `-1`로 표현한다.
응답은 coordinator Future 이후에만 전송하므로 등록/할당의 성공 응답은 해당 인덱스 기록의
커밋을 보장한다. 실제 브로커 테스트는 세 RPC, 커밋된 배치의 중복 요청,
epoch 불일치 거절, 재시작 후 fence·progress 복원을 검증한다.

## 7. 정상 인덱싱과 append 판단

1. Indexer는 소유권과 committed progress를 확인한다.
2. 마지막 커밋된 배치의 `physicalLastOffset + 1`부터 데이터 로그를 순회한다.
3. Control batch를 건너뛰고, data HW 아래의 다음 완전한 대상 배치를 선택한다.
4. PhysicalBatchId, 배치 범위와 개수, 현재 소유권, 직전 대상 배치 식별자를 제출한다.
5. Coordinator의 하나의 write 이벤트에서 아래 판단과 신규 할당을 수행한다.
6. 인덱스 write가 커밋되면 progress를 전진시키고 다음 배치를 진행한다.

```text
소유권이 오래됨                         -> Fenced
유효한 요청 범위가 committed prefix 안  -> AlreadyIndexed
동일 배치가 speculative/pending 상태    -> 기존 할당의 커밋에 대기 연결
다른 배치가 pending 또는 predecessor 불일치 -> OutOfOrder / 진행 위치 재조회
현재 진행 위치 다음의 대상 배치         -> 신규 범위 생성 및 replay/append
```

Batch identity가 같지만 현재 보유한 배치의 last offset이나 count가 다르면 오류다.
범위 중첩, 잘못된 배치 경계, 음수 및 overflow는 유효한 신규 요청으로 취급하지 않는다.
오래된 AlreadyIndexed 요청은 신뢰된 데이터 리더가 실제 원본 배치에서 만든 요청이라는
계약을 따른다. 완료 응답이 과거 모든 배치 메타데이터의 재검증을 의미하지는 않는다.

원본 로그의 누락 없는 순회는 데이터 리더의 Indexer가 책임진다. Predecessor 검사는
전달 순서와 재시도 순서를 검증하는 장치이며, 임의의 physical offset 사이에 실제 배치가
없는지를 독립적으로 증명하는 장치는 아니다. Control batch 때문에 physical 위치에
숫자상의 간격이 있어도 직전 대상 배치가 맞으면 정상이다.

이미 처리한 prefix를 판단할 때 cache나 과거 인덱스 로그를 scan하지 않는다.
사전 read의 신규 판정에 의존해 나중에 무조건 할당하는 경로도 만들지 않는다.
Index HW가 진행되거나 cache에서 항목이 사라져도 같은 prefix에 새 번호를 주지 않는다.

## 8. 내부 결과와 Produce 응답

내부 결과의 wire 표현은 아래 v0 RPC 계약을 따른다. `ErrorCode=NONE`만으로 append 성공이나 소유권 획득을 판단하지 않는다.

| 결과 | 계약 |
|---|---|
| Indexed | 이번 논리적 요청의 인덱스가 커밋됨 |
| AlreadyIndexed | 해당 유효 배치 범위가 committed progress에 포함됨 |
| Fenced | 호출자는 더 이상 소유자가 아니므로 진행 중단 |
| OutOfOrder | 현재 진행 상태와 맞지 않으므로 앞선 작업 해결 또는 상태 재조회 |
| Retryable failure | 같은 논리적 요청을 유지하고 재시도 |
| Unrecoverable gap | 필요한 원본/인덱스가 사라졌으므로 해당 파티션 진행 중단 및 오류 노출 |

Indexed와 AlreadyIndexed는 요청 식별 정보와 committed progress를 확인할 수 있어야 한다.
AlreadyIndexed에 과거의 정확한 global base offset을 반드시 반환하지 않는다. 그 값이
필요한 사용자는 별도의 매핑 조회를 사용한다. Pending은 성공 결과가 아니며, timeout이
발생하더라도 같은 배치의 할당을 보존하고 새 대기 요청을 연결할 수 있어야 한다.

`KafkaApis`는 데이터 append의 physical base offset과 실제 배치 끝으로 완료 waiter를
등록한다. 먼저 progress를 확인하고 알림을 구독한 뒤 재확인하는 등, waiter 등록 전에
인덱싱이 끝난 경우와 등록 도중 완료되는 경우 모두 알림을 놓치지 않도록 한다.
Producer payload를 인덱스 로그에 복사하지 않으며 요청 스레드를 블로킹하지 않는다.

| Produce acks | 응답 계약 |
|---|---|
| 1 | 성공 응답은 데이터 HW 및 해당 인덱스 커밋 이후. 일반 토픽의 acks=1보다 강한 조건 |
| all / -1 | 기존 데이터 복제 조건과 해당 인덱스 커밋 이후 성공 |
| 0 | Produce 응답 메시지는 없음. 백그라운드 인덱싱은 동일하게 진행 |

Produce 응답의 base offset은 항상 physical offset이다. Global/일반 토픽이 섞인 요청도
파티션별 결과를 유지하고, 응답이 필요한 요청은 해당 global 파티션들의 완료를 기다린다.
대역폭·요청 quota 처리는 유지한다.

데이터 append와 인덱스 대기에 하나의 broker 요청 deadline을 적용하며, 단계가 바뀔 때
대기 예산을 새로 시작하지 않는다. Client delivery timeout은 이와 별개다. Deadline이
끝나면 waiter를 해제하고 오류를 반환하되 Indexer와 이미 기록한 데이터를 취소하지 않는다.
`acks=0` 요청의 수신·append 오류 처리는 기존 Kafka 동작을 따르며, 요청 종료 후 발생한
백그라운드 인덱스 실패는 복구·지표로 처리한다. 이미 반환한 클라이언트 성공을 취소하는
개념은 두지 않는다.

## 9. 복구와 미확정 write

### 인덱스 리더의 복구

인덱스 로그를 replay하여 토픽별 sequence와 데이터 파티션별 최신 상태를 재구성한다.
Index HW 미만의 committed 기록에서 외부에 제공할 progress를 복원한다. Local log에
남은 미확정 tail은 speculative 상태로 추적하거나 로그 복구에 따라 제거한다.
단순히 committed scan에서 못 찾았다는 이유로 tail에 있는 배치를 새로 할당하지 않는다.

Snapshot과 pending 상태는 같은 로그 이력을 설명해야 한다. Local append가 실제로
실패하여 rollback되면 관련 speculative 상태를 함께 되돌린다. Timeout만 난 경우에는
그렇게 하지 않는다. 재시도는 실패 완료된 옛 Future 객체만 재사용하지 않고, 기존 할당의
현재 로그·커밋 상태에 새 완료 대기를 연결한다.

### 데이터 리더의 복구

새 Indexer는 소유권 등록 barrier를 통과한 뒤 committed progress를 조회한다.
마지막 대상 배치의 끝 다음부터 원본 로그를 순회한다. 인덱스가 없는 파티션은 시작 위치
0부터 진행한다. 마지막 인덱스 뒤의 control-only 구간은 다시 읽어도 되며, 이를 위해
별도의 영속 cursor를 반드시 기록하지 않는다.

인덱스 기록들은 데이터 파티션별 전용 로그에 따로 존재하지 않는다. 같은 인덱스 파티션에
여러 데이터 파티션의 기록이 섞인다. 따라서 끝의 고정된 N개 레코드만 읽고 오래 조용했던
파티션을 신규로 판단해서는 안 된다. 충분한 replay/scan 또는 검증 가능한 checkpoint로
각 파티션의 진행 위치를 복원해야 한다.

긴 조회에서 캡처한 progress는 재개 힌트이며 append 판단의 최종 권위가 아니다.
Coordinator는 실제 write 이벤트의 current committed/pending 상태로 재확인한다.
Index HW가 증가한 경우 다른 파티션의 진행만으로 복구를 무조건 처음부터 반복할 필요는
없지만, 현재 진행 상태를 확인하지 않은 오래된 결과로 신규 범위를 만들 수는 없다.

### 데이터 Indexer 복구 구현

원본 Reader에서 `SourceLogGapException`을 받으면 바로 `logStartOffset`으로 이동하지 않는다.
같은 registration UUID/generation으로 등록 barrier를 재확인하고, 그 barrier의 route token과
일치하는 committed progress를 조회한다. 진행 위치가 더 앞서 있다면 그 prefix 이후로
이동하여 원본 로그를 다시 확인한다. Control-only 구간을 이미 검증한 같은 source 수명에서는
그 임시 읽기 위치를 유지하며, 새 source 수명에서는 영속 progress 다음부터 다시 읽는다.

권위 있는 진행 위치를 확인한 뒤에도 Reader가 동일 인덱스 리더에서 gap을 보고하면
해당 Indexer를 오류로 중단한다. 재확인 사이에 인덱스 리더가 바뀌면 이전 조회로 손실을
확정하지 않고 새 리더의 barrier와 progress를 다시 확인한다. 복구 중 CAS 거절이나
ownership 변경을 받으면 이전 Indexer를 중단하며 다른 generation으로 소유권을 되찾지 않는다.

Append의 `OUT_OF_ORDER`도 같은 복구 절차를 적용한다. 확정 progress가 진행 중 배치를
포함하면 이미 처리된 배치를 제거하고 다음 위치로 진행한다. 그렇지 않으면 원래 배치·
predecessor·identity를 유지해 재전송하며, 재확인한 동일 리더에서도 순서 오류가 반복되면
중단한다. Progress의 후퇴나 확정 prefix와 pending predecessor의 모순도 오류로 처리한다.
단순 timeout과 `OWNER_NOT_COMMITTED`는 이 unrecoverable 판정과 구분하여 기존 재시도/
등록 barrier 경로를 따른다.

통합 테스트는 A 브로커에서 이미 인덱싱한 prefix 뒤에 데이터만 복제한 상태를 만든 뒤,
데이터 리더를 B로 옮겨 미인덱싱 배치를 복구하고 다시 A로 옮겨 다음 배치까지 진행한다.
이전 Indexer의 source epoch와 실행 수명을 재사용하지 않으며, 늦은 callback은 새 수명에
적용되지 않는다. 브로커 재시작 및 인덱스 리더 이동 테스트와 함께 이 경로를 검증한다.

## 10. 보존과 데이터 손실 감지

미인덱싱 대상 배치가 retention 또는 DeleteRecords로 먼저 사라지지 않도록 삭제 경계를
제한한다. 리더뿐 아니라 승격 가능한 follower에도 적용하며, 시작 시 완료 위치를 모르면
새 삭제를 보류한다. 보존 경계 갱신에는 committed progress만 사용한다.

Control-only suffix의 재스캔 때문에 보존이 잠시 더 길어지는 것은 허용한다. 인덱싱이
막히면 삭제도 제한될 수 있으므로 저장 공간, lag, backpressure를 관측해야 한다.

복구할 다음 위치가 log start offset보다 앞이라면 coordinator의 권위 있는 progress를
다시 확인한다. 그래도 필요한 범위를 복원할 수 없으면 Unrecoverable gap으로 멈춘다.
Log start offset으로 임의 이동하여 누락 없는 prefix라는 보장을 깨지 않는다.

이미 인덱싱한 원본 데이터는 일반 retention 정책에 따라 만료될 수 있다. 그 데이터를
요청하는 global fetch는 범위 오류를 반환하고 이를 빈 결과나 정상 페이지 이동으로 숨기지
않는다. 토픽 삭제는 정상적인 lifecycle 종료이며, 대기 작업과 소유권 상태를 정리한다.
같은 이름으로 재생성한 토픽은 새 topic ID와 새 global sequence를 사용한다.

1차 버전은 인덱스 이력을 임의로 삭제하지 않는다. TopicMetadata와 IndexerFence는 key별
최신 값을 유지할 수 있으며 BatchIndex key는 global base offset으로 구분된다. 향후 GC를
추가하려면 파티션별 마지막 progress와 next global offset을 잃지 않는 checkpoint 계약을
먼저 정의해야 한다. Checkpoint는 초기 구현의 정확성 전제 대신 복구 비용 최적화로 둔다.

## 11. Global 읽기 계약

### 인덱스 조회

Global lookup은 topic ID와 `[globalStartOffset, globalEndOffsetExclusive)` 및 페이지
제한을 입력받아 global 순서의 BatchIndex를 반환한다. 배치 중간에서 시작하거나 끝나는
요청도 표현한다. 동일 snapshot의 committed global end와 다음 조회 위치를 반환한다.

요청 끝이 committed global end를 넘으면 현재 커밋된 범위까지만 제공한다. 시작 위치가
committed global end와 같으면 빈 결과를 반환하며, 그보다 크면 범위 오류다. 음수나
start > end는 잘못된 요청이고, 유효한 빈 범위는 빈 결과다. Pagination으로 일부만 반환할
때는 다음 미처리 위치를 명시한다. 미커밋 할당은 노출하지 않는다.

### 데이터 읽기

Global fetch는 인덱스 조회 후 각 physical 데이터 리더에서 배치를 읽는다. Physical
읽기는 제한된 병렬 실행이 가능하지만 결과는 global 순서로 조립한다. 앞선 읽기가 아직
완료되지 않았거나 실패했으면 뒤의 데이터를 먼저 반환하고 cursor를 넘기지 않는다.

응답에는 원본 배치와 physical/global 매핑, 배치 내 선택 범위, `nextGlobalOffset`을
표현한다. 기존 record batch의 physical offset을 global offset으로 덮어써 CRC나 producer
메타데이터를 변경하지 않는다. `maxBytes`는 반환하는 원본 배치 바이트의 합에 적용하고
배치를 절단하지 않는다. 읽기 진행을 위해 첫 번째 반환 대상 배치 하나는 `maxBytes`를
초과해도 허용하며, 이후 배치는 한도 안에서만 추가한다. 배치 중간 범위만 선택해도
전달하는 전체 배치 크기를 계산한다. 프로토콜의 별도 응답 크기 상한 때문에 첫 배치도
전달할 수 없으면 오류를 반환하고 cursor를 전진시키지 않는다.

대상 토픽 READ 권한, request/bandwidth quota, 원격 리더 변경과 응답 크기 제한을
적용한다. Client가 요청한 토픽을 읽을 권한과 브로커 내부 physical 읽기의
CLUSTER_ACTION 권한은 각 진입점에서 구분한다.

### 트랜잭션 격리

READ_UNCOMMITTED는 커밋된 인덱스와 data HW 아래의 데이터를 기준으로 하므로 열린
사용자 트랜잭션의 배치도 포함할 수 있다. READ_COMMITTED는 physical LSO와 abort 정보를
사용한다. 아직 commit/abort가 확정되지 않은 첫 global 구간에서 멈추며, 뒤 파티션의
확정된 데이터를 먼저 반환하지 않는다.

Abort가 확인된 배치는 global 범위를 소비한 채 필터링한다. 다음 cursor에는 필터링을
반영하고 번호를 재사용하지 않는다. 반환 레코드의 global offset에 이로 인한 빈 구간이
있을 수 있다. 인덱스 커밋이 사용자 트랜잭션 완료를 뜻하지 않는다.

## 12. 장애 시나리오와 기대 결과

아래 ID는 후속 단위·통합 테스트의 인수 기준이다. 표의 범위는 원본과 인덱스가
보존되고 정상 복제 복구가 가능한 경우다.

| ID | 제어할 실행 순서 | 기대 결과 |
|---|---|---|
| S01 | 같은 파티션 A 저장, A 인덱싱 전에 B 저장 | Indexer는 A 커밋 확인 후 B 진행. B의 선행 제출은 순서 오류 |
| S02 | P0의 A/B와 P1의 C/D가 동시에 도착 | 토픽 global 범위는 겹치지 않고 A<B 및 C<D 유지 |
| S03 | 데이터 커밋 후 인덱스 요청 전에 브로커 종료 | 복구한 progress 다음부터 원본을 읽어 누락 배치 인덱싱 |
| S04 | 인덱스 replay 후 로컬 append 실패 | speculative 범위·진행 상태 rollback. 미커밋 할당의 성공 응답 없음 |
| S05 | 인덱스 로컬 append 후 복제 지연으로 timeout, 이후 HW 상승 | 재시도도 기존 할당에 연결. 커밋된 범위는 하나 |
| S06 | 인덱스 커밋 후 응답 유실 | AlreadyIndexed 반환. 새 범위나 과거 매핑 scan 불필요 |
| S07 | 오래된 배치 cache eviction 후 재시도 | committed progress로 AlreadyIndexed. cache 크기와 무관한 결과 |
| S08 | 데이터 리더 A→B→A, 첫 A의 요청을 지연 전달 | 현재 소유권보다 오래된 요청 거절. 커밋된 physical identity 유지 |
| S09 | 등록 응답 유실 또는 옛 등록 요청의 지연 도착 | 동일 등록의 재시도는 멱등. 새 소유권을 옛 등록이 덮어쓰지 않음 |
| S10 | 인덱스 리더 종료, committed prefix와 미확정 tail이 있는 로그 재로딩 | committed progress만 공개. tail을 중복 할당하지 않고 커밋/제거와 조정 |
| S11 | 복구 scan 중 인덱스 리더 epoch 변경 | 오래된 결과 검증 실패 및 새 리더에서 재조회 |
| S12 | 한 데이터 파티션이 오래 조용한 상태에서 복구 | 최신 몇 개만 보고 미처리로 오판하지 않음 |
| S13 | Indexer가 멈춘 동안 retention/DeleteRecords 및 follower 승격 | 필요한 미인덱싱 데이터 보존. 보존이 깨졌다면 오류로 중단 |
| S14 | Produce waiter 등록 직전 또는 도중 인덱싱 완료 | 완료를 놓치지 않고 physical offset으로 성공 응답 |
| S15 | Produce timeout 후 background 인덱싱 완료 및 Producer 재시도 | 원래 작업은 계속 진행. 같은 물리 배치는 중복 할당하지 않음 |
| S16 | Global fetch에서 뒤 파티션의 RPC가 먼저 완료 | global 순서로 반환. 앞 구간 실패를 건너뛰지 않음 |
| S17 | 앞 global 배치는 열린 트랜잭션, 뒤 배치는 확정됨 | READ_COMMITTED는 앞 구간에서 멈춤. commit/abort 확정 후 재개 |
| S18 | 트랜잭션 abort, control batch, 배치 중간 페이지 경계 | 번호 재사용 없음. 필터링과 next cursor가 중복·누락 없이 일치 |
| S19 | 한 토픽 삭제 후 같은 이름으로 재생성 | 이전 topic ID의 요청 거절. 새 topic ID는 0부터 시작 |
| S20 | 기능 비활성 토픽, mixed-topic Produce, 권한 없는 global 읽기 | 기존 토픽 동작 유지. 파티션별 오류와 권한 검사 유지 |

대표 timeout 시나리오 S05의 상세 순서는 다음과 같다.

```text
t0: 데이터 배치 B가 data HW 아래에 있음
t1: B -> global [10, 13) 할당, 인덱스 로그 append
t2: index HW는 아직 해당 write 이전, RPC timeout
t3: 같은 B 재시도 -> pending [10, 13)에 새 대기 연결
t4: index HW 상승 -> B의 단일 할당 커밋
t5: 현재 waiter 성공. 이미 timeout 처리한 옛 요청의 응답을 다시 보내지 않음
```

각 구현 커밋에서 해당 시나리오를 테스트한다. 무작위 sleep으로 재현에 의존하지 않고
append·HW 갱신·응답 전달 지점을 제어한다. 최종 장애 테스트에서는 보존된 대상 배치의
매핑이 하나씩 존재하고, 토픽 범위가 겹치지 않으며, 파티션별 순서가 유지되는지 비교한다.

## 13. 기존 Kafka와의 연결 및 후속 작업

기준 브랜치에서 재사용하거나 확장할 지점은 다음과 같다.

- [KafkaApis](../../core/src/main/scala/kafka/server/KafkaApis.scala):
  Produce response callback에 인덱싱 완료 대기를 연결한다.
- [CoordinatorRuntime](../../coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java):
  write 실행 시 HW·epoch context, HW/rollback hook을 추가한다. 빈 레코드 결과도
  pending write를 기다리는 기존 계약을 고려하며, 이 동작을 임의로 우회하지 않는다.
- [CoordinatorLoaderImpl](../../core/src/main/scala/kafka/coordinator/group/CoordinatorLoaderImpl.scala):
  로딩 중 replay한 위치와 committed 위치를 구분하여 상태를 복원한다.
- [CoordinatorPartitionWriter](../../core/src/main/scala/kafka/coordinator/group/CoordinatorPartitionWriter.scala):
  로컬 append와 HW 알림을 재사용한다.
- [Partition](../../core/src/main/scala/kafka/cluster/Partition.scala):
  HW와 leadership listener를 Indexer의 시작·중단·깨우기에 사용한다.
- [BrokerMetadataPublisher](../../core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala):
  인덱스 coordinator와 데이터 Indexer의 metadata lifecycle을 연결한다.
- [UnifiedLog](../../storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java):
  복구에 필요한 데이터가 삭제되지 않도록 retention 경계를 연결한다.

저장 형식과 공통 runtime, coordinator 상태와 내부 RPC, 순차 Reader/Indexer까지 구현했다.
복구의 progress 재확인과 source leader 변경을 연결했으며, 후속 구현 순서는
원본 보존, Produce 대기, global 조회/읽기,
트랜잭션 격리, 처리량 제어와 종합 검증이다. 현재 shard 및 Runtime 연동 테스트는
할당의 원자성, 파티션 간 순서, committed/pending 분리, timeout 뒤 재시도,
append 실패·rollback과 로그 replay를 검증한다. 브로커 재시작과 인덱스 리더 이동은 통합 테스트로 검증하며, 종합 장애 시나리오는 후속 단계다.

후속 global 읽기 프로토콜의 API 번호·wire 필드, checkpoint 형식,
배치 묶기 크기와 지표 이름은 해당 구현 커밋에서 확정한다. 이 선택들이 위의 순서,
커밋, fencing, timeout, 복구 계약을 약화해서는 안 된다.
