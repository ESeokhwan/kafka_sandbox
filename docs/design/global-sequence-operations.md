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

# Global sequence topic 사용·운영 가이드

이 문서는 이 브랜치의 Kafka 4.1.1 기반 구현에 해당한다. [상세 설계](global-sequence.md),
[구현 계획](global-sequence-implementation-plan.md), [장애 검증 목록](global-sequence-validation.md)을 함께 참고한다.

## 1. 배포 조건과 토픽 생성

Controller와 모든 broker에 이 브랜치의 코드를 배포해야 한다. 읽기 클라이언트도 같은 브랜치의
`kafka-clients`를 사용한다. Vanilla Kafka와 혼합한 rolling upgrade, 다른 API 번호를 사용하는
fork와의 호환성은 지원 범위가 아니다. 일반 Producer와 physical KafkaConsumer API는 유지되지만,
표준 KafkaConsumer에 global offset을 전달해 global 읽기를 할 수는 없다.

아래는 3개 이상의 broker가 있는 KRaft 클러스터 예다. 모든 broker에 같은 정적 설정을 적용한다.
내부 인덱스 토픽은 자동 생성된다. 이미 생성된 인덱스 토픽의 파티션 수는 변경하지 않는다.

```properties
global.sequence.coordinator.index.topic.num.partitions=50
global.sequence.coordinator.index.topic.replication.factor=3
global.sequence.coordinator.index.topic.min.isr=2
```

```sh
./gradlew :core:jar :tools:jar :examples:jar :core:copyDependantLibs
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic global-events --partitions 3 --replication-factor 3 \
  --config global.sequence.enabled=true --config cleanup.policy=delete \
  --config min.insync.replicas=2 --config unclean.leader.election.enable=false
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic global-events
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic __global_sequence_index
```

생성 시에만 활성화할 수 있다. 기존 토픽의 활성화·비활성화, 활성 토픽의 compaction 전환은
거절된다. 위 describe 결과의 **TopicId**를 이후 조회에 사용한다. 같은 이름으로 삭제·재생성하면
UUID가 바뀌고 global offset은 새로 0부터 시작한다. 체크포인트는 `(topic UUID, next global offset)`으로 저장한다.

인덱스 토픽의 설정은 `cleanup.policy=delete`, `retention.ms=-1`, `retention.bytes=-1`,
`unclean.leader.election.enable=false`를 유지한다. 이 토픽에 수동 Produce, DeleteRecords,
삭제·재생성, compaction 또는 파티션 수 변경을 수행하지 않는다. 전체 인덱스 이력이 복구와
global lookup의 근거이며, checkpoint·GC는 아직 구현하지 않았다.

## 2. Produce와 성공 응답의 의미

```sh
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic global-events \
  --producer-property acks=all --producer-property enable.idempotence=true
```

| acks | 활성 토픽의 응답 의미 |
|---|---|
| `all` / `-1` | 데이터 HW와 해당 배치의 인덱스 커밋을 확인한 뒤 성공 |
| `1` | 이 기능에서는 데이터 HW와 인덱스 커밋까지 대기. 일반 토픽의 acks=1보다 강한 조건 |
| `0` | 성공 응답이나 인덱싱 완료 대기 없음. 커밋된 데이터는 백그라운드 인덱싱 |

Produce 응답의 offset은 **physical offset**이다. 다른 파티션에서 동일한 physical offset이
나올 수 있다. Global 순서는 인덱스 할당 순서이고 이벤트 시간이나 여러 producer의 호출 시각
순서와 같다고 보장하지 않는다. 같은 데이터 파티션의 배치 순서는 보존한다.

Timeout은 결과가 미확정임을 뜻한다. 이미 저장된 데이터와 인덱스 write는 취소되지 않으며,
백그라운드 인덱싱도 계속된다. 동일 physical batch의 재시도는 새 global 범위를 할당하지 않는다.
동일한 메시지를 **새로운 physical batch로** 다시 쓰는 것까지 제거하지는 않는다. Producer의
idempotence와 애플리케이션의 재처리 정책을 함께 사용한다. 트랜잭션 Produce 성공도 transaction의
commit 완료를 뜻하지 않는다. Abort된 데이터에도 할당된 global 번호는 재사용하지 않는다.

## 3. Lookup과 global 데이터 읽기

[GlobalSequenceReadDemo](../../examples/src/main/java/kafka/examples/globalsequence/GlobalSequenceReadDemo.java)는
실제로 컴파일되고 장애 통합 테스트에서 실행되는 raw protocol 예제다. 임의의 broker에 요청하면
broker가 index/source leader로 라우팅한다. ApiVersions로 버전을 협상하며, `read_committed`는
v1이 없을 때 실패한다. 격리 수준을 자동으로 낮추지 않는다.

`TOPIC_ID`를 describe 결과의 TopicId로 바꾼다. 범위는 `[start, end-exclusive)`다.

```sh
TOPIC_ID='describe에서-확인한-TopicId'
bin/kafka-run-class.sh kafka.examples.globalsequence.GlobalSequenceReadDemo \
  localhost:9092 "$TOPIC_ID" 0 100 lookup
bin/kafka-run-class.sh kafka.examples.globalsequence.GlobalSequenceReadDemo \
  localhost:9092 "$TOPIC_ID" 0 100 read_uncommitted
bin/kafka-run-class.sh kafka.examples.globalsequence.GlobalSequenceReadDemo \
  localhost:9092 "$TOPIC_ID" 0 100 read_committed client.properties
```

마지막 `client.properties` 인자는 선택 사항이며 AdminClientConfig의 SASL/SSL 설정을 사용한다.
공개 lookup/fetch에는 데이터 토픽 `READ` 권한이 필요하다. 내부 RPC에는 `CLUSTER_ACTION`이
필요하므로 broker 간 principal에도 부여한다. 예제는 최초 연결 시 bootstrap 목록을 순서대로
시도한다. 이후 연결/서버 오류는 호출자에게 반환한다. 자동 재시도·consumer group·체크포인트
저장 기능이 있는 production consumer가 아니라 명시적 cursor 사용을 보여주는 유한 읽기 예제다.

예제의 탭 구분 출력은 다음과 같다. Value는 binary 데이터를 안전하게 표시하도록 Base64이며
null value는 문자열 `null`로 표시한다.

```text
mapping <globalBase> <partition> <physicalBase> <physicalLast> <selectedStart> <selectedEnd>
record <globalOffset> <partition> <physicalOffset> <base64Value>
page next=<cursor> committedEnd=<index-committed-end> pending=<true|false> error=<error>
```

예제는 최대 2배치씩 페이지를 요청하고 첫 페이지의 committed global end까지로 읽기 끝을 고정한다. 쓰기가 계속되는 토픽에서도
종료하며, 열린 transaction이나 오류가 있으면 해당 cursor를 출력하고 중단한다. 서버 오류에서는
앞서 출력한 유효 prefix를 유지하고 예외로 종료한다. 연결 오류로 현재 페이지 응답 자체를 받지
못했다면 마지막으로 처리한 페이지의 cursor부터 재시도한다. stdout 출력과 외부 처리의 원자성은
제공하지 않으므로 실제 애플리케이션은 처리 완료와 체크포인트 저장을 직접 연결해야 한다.

### Cursor와 원본 배치 처리

Fetch는 CRC·physical offset·producer 정보를 보존한 **완전한 원본 배치**를 반환한다.
각 record의 global offset은 다음 식으로 계산한다.

```text
global = mapping.globalBaseOffset + (record.offset - mapping.physicalBaseOffset)
selectedGlobalStartOffset <= global < selectedGlobalEndOffset 인 record만 처리
```

1. 반환된 배치를 global 순서로 처리하되 배치별 selected 범위를 적용한다.
2. 처리 완료 후 응답의 `NextGlobalOffset`을 저장한다. 레코드 수나 마지막 레코드 offset으로 추정하지 않는다.
3. 다음 페이지는 그 cursor에서 요청한다. 오류가 있어도 앞서 반환된 연속 prefix는 유효할 수 있다.
4. `TransactionPending=true`이면 첫 미확정 구간에서 멈춘 상태다. 같은 cursor에서 backoff 후 재조회한다.
5. 레코드가 없는 페이지도 abort된 범위를 소비하여 cursor가 전진할 수 있다. 빈 목록만 보고 종료하지 않는다.

`CommittedGlobalEndOffset`은 인덱스의 커밋 경계다. `READ_COMMITTED`에서 읽을 수 있는 범위와는
다르다. 앞 global 구간이 source LSO에 걸리면 뒤 파티션이 확정되어 있어도 앞에서 멈춘다.
Commit/abort marker가 복제되어 LSO가 전진하면 재개한다. Abort는 global 번호의 빈 구간으로 남고,
control batch에는 global 번호를 할당하지 않는다. Lookup은 transaction 필터링 없이 매핑을 반환한다.

공개 API는 `LookupGlobalSequence`(96, v0), `FetchGlobalSequence`(98, v0~v1)다.
Fetch v0은 READ_UNCOMMITTED만, v1은 두 격리 수준을 지원한다. 내부 API는
93~95(register/describe/append), 97(index read), 99(data read)이며 일반 클라이언트가 직접 호출하지 않는다.
`MaxBatches`는 1~1000, `TimeoutMs`는 1~30000이다. Fetch `MaxBytes`는 기본 1 MiB의 soft limit이며
첫 완전 배치가 초과할 수 있다. Physical 배치 hard limit은 8 MiB, 전체 응답은 16 MiB이고,
요청 payload 상한은 메타데이터 여유를 뺀 `16 MiB - 128 KiB`다. 기본 source reader의 hard copy
limit도 8 MiB이므로 이를 넘는 배치를 허용하기 전에 읽기·인덱싱 제한을 함께 검토한다.

## 4. 오류와 재시도

| 상태 | 의미와 대응 |
|---|---|
| `REQUEST_TIMED_OUT`, 연결 종료 | 쓰기 결과가 미확정일 수 있다. Producer idempotence 유지. 읽기는 처리한 prefix의 cursor부터 재시도 |
| `COORDINATOR_LOAD_IN_PROGRESS`, `NOT_COORDINATOR`, leader/epoch 오류 | index/source 리더 전환 중일 수 있다. 동일 UUID·cursor로 다른 정상 broker에 backoff 재시도 |
| `THROTTLING_QUOTA_EXCEEDED` | admission 또는 scan 한도. 요청 동시성·속도·batch 크기를 줄이고 지표 확인. Cold lookup scan 한도라면 반복만으로 해결되지 않음 |
| `TransactionPending=true`, `NONE` | 오류가 아니라 LSO 대기. transaction 종료와 복제 상태 확인 후 같은 cursor로 재개 |
| `OFFSET_OUT_OF_RANGE` | 매핑의 원본이 retention/DeleteRecords로 이미 삭제되었을 수 있다. 앞 prefix만 처리하고 중단. 자동으로 뒤 global 범위를 건너뛰지 않음 |
| `UNKNOWN_TOPIC_ID` | 토픽 삭제 또는 오래된 UUID. 이름을 다시 조회해도 이전 cursor를 새 UUID에 재사용하지 않음 |
| `TOPIC_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED` | 사용자 또는 broker 간 권한 수정 필요 |
| `INVALID_REQUEST`, `UNSUPPORTED_VERSION` | 범위·페이지 크기·격리 수준·버전 및 모든 broker의 빌드 확인 |
| `MESSAGE_TOO_LARGE`, `RECORD_LIST_TOO_LARGE`, 손상/일관성 오류 | 배치·segment 제한과 로그 보존·무결성 확인. 자동 offset 점프나 인덱스 재생성으로 우회하지 않음 |

`FENCED_LEADER_EPOCH`는 오래된 source lifetime을 막는 정상 동작일 수 있다. Indexer가 소유권을
잃으면 중단하고 현재 leader의 새 lifetime이 복구한다. 옛 owner token을 바꿔 재제출하는 식으로
fencing을 무력화하지 않는다. Broker의 내부 재시도는 batch·predecessor·ownership을 유지한다.

## 5. 복구와 retention 운영

정상 복제 복구 가능한 장애에서는 수동 인덱싱 시작 offset을 지정할 필요가 없다. 새 source
leader는 조건부 등록 barrier를 커밋하고 authoritative committed progress를 확인한 뒤,
그 다음 physical 배치부터 읽는다. Index leader는 전체 보존 로그를 replay하여 committed prefix와
pending tail을 분리한다. 오래 조용했던 파티션도 최신 몇 개의 인덱스만 보고 진행 위치를 추정하지 않는다.

장애 후에는 다음 순서로 확인한다.

1. 데이터와 `__global_sequence_index`의 leader·ISR 및 index HW 진행을 확인한다. 필요한 replica를 복구한다.
2. Indexer의 복구 완료, indexing lag 감소, Produce timeout 해소를 확인한다. 기존 global cursor에서 읽기를 재개한다.
3. Lag가 멈추면 source UUID/epoch, ACL, worker/admission 포화, read timeout, 배치 크기, source gap 로그를 확인한다.
4. 미인덱싱 원본 누락이나 index 이력 손상이면 해당 토픽 복구를 중단하고 보존된 정상 replica/백업을 조사한다.
   같은 이름의 빈 인덱스를 만들어 복구를 계속하는 절차는 제공하지 않는다.

모든 source replica와 future log가 committed progress에 따른 삭제 경계를 보유한다.
진행 위치를 아직 모르면 0에서 보수적으로 보존한다. Indexer가 멈췄을 때 retention이 미인덱싱 데이터를
삭제하지 않으며 DeleteRecords도 경계를 넘으면 거절한다. 이 때문에 디스크 사용량이 retention 설정보다
커질 수 있다. 복구 후 경계가 전진하면 통상 retention을 재개한다.

이미 인덱싱된 원본은 데이터 토픽 retention/DeleteRecords로 삭제될 수 있다. 인덱스가 남아 있어도
원본이 없으면 global Fetch는 읽을 수 없다. 필요한 global 읽기 기간에 맞춰 데이터 보존 기간을
설정한다. 인덱스 무기한 보존은 데이터 무기한 보존을 뜻하지 않는다.

## 6. 지표와 자원 제한

Metrics group은 `global-sequence-resources`, tag는 고정된 `scope`다.
정확한 기본값·scope 키·수명은 [설계 §14](global-sequence.md#14-17번-구현-자원-제한과-관측)를 따른다.

| 관측 | 해석 |
|---|---|
| `physical-offsets`, scope=`indexing_lag` | Local source leader의 data HW와 committed resumeOffset 차이 합. 지속 증가 시 index ISR/HW·복구·부하 확인 |
| `physical-offsets`, scope=`retention_held` | Hosted current/future log의 data HW와 삭제 경계 차이 합. 높은 값과 디스크 증가가 함께 나타나는지 확인 |
| `pending`, `reserved-bytes` | 실행·커밋·실제 네트워크 완료를 기다리며 점유 중인 수/예약량. 현재 payload 바이트 측정값은 아님 |
| `rejected-total`, `errors-total` | scope별 한도 초과와 실패. 증가율 및 요청 latency와 함께 확인 |
| `total`, scope=`rpc_retry`, `worker_retry`, `fenced`, `gap` | 재시도, 큐 재제출, 소유권 차단, 복구 불가능 gap으로 중단한 lifetime 수 |
| `duration-ms-avg/max`, scope=`recovery` | 등록 barrier 및 committed progress 재확인의 복구 시간 |

Offset 지표는 **physical offset 차이**이며 control offset도 포함한다. 서로 다른 replica의 값은
중복 기여할 수 있다. Global offset이나 바이트로 해석하지 않는다. 정상 failover에서도 fencing과
retry 누적값은 늘 수 있으므로 누적값 자체보다 증가율과 복구 성공·lag를 함께 본다.

대표 기본값은 scope별 pending 1024, 키별 pending 64, Produce waiter 10000,
worker queue 128이다. Fetch 예약 예산 256 MiB에서 한 요청은 48 MiB를 예약하므로 바이트 한도가
동시성을 먼저 제한할 수 있다. Data RPC와 internal read response는 각각 128 MiB의 독립 예산이다.
Timeout/cancel 이후 실제 write commit이나 I/O가 끝날 때까지 자원이 남는 것은 의도된 동작이다.
Client를 반복 취소하는 것으로 점유된 메모리가 즉시 사라지지는 않는다.

Lookup은 현재 index 처음부터 scan하며 누적 기본 한도는 256 MiB다. 긴 이력에서 계속 같은
`THROTTLING_QUOTA_EXCEEDED`가 나면 페이지 크기를 줄이거나 같은 cursor를 재시도하는 것만으로
해결되지 않을 수 있다. 부하·메모리·deadline을 검토해 정적 `global.sequence.lookup.scan.max.bytes`를
조정하거나 후속 seek/checkpoint 구현이 필요하다. 모든 브로커의 정적 설정을 일관되게 관리한다.

## 7. 지원 범위와 한계

1~18번은 Produce·복구·global 읽기의 1차 구현 범위다. 이는 전원 손실, 파일시스템 손상,
모든 replica의 커밋 데이터 유실까지 복구한다는 보장이 아니다. 장애 테스트는 실제 broker lifecycle과
복제 제어, 별도 Runtime/Future 주입을 사용하며 상세 범위는 [검증 문서](global-sequence-validation.md)에 기록한다.

Global consumer group/offset commit 통합, 기존 토픽 마이그레이션, compaction,
index checkpoint·GC·직접 seek, 원격 저장소에만 남은 원본의 복구는 후속 범위다.
이력 길이에 따른 scan 비용과 무기한 index 저장 비용을 운영 용량에 포함한다.
