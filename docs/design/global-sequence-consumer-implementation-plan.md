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

# Global sequence consumer 구현 계획

작성 기준: 2026-09-16, `proj/globally_ordered_topic/develop_v4`, C06 구현 작업 기준.
출발 API 커밋은 `672277695e`다.
요구사항은 [examples/README.md](../../examples/README.md)의 `Global sequence consumer` 절이다.
기존 broker 구현 1~18번 이후의 **별도 client 계획 C01~C07**이다.
**C01~C07을 완료했다.**

## 1. 현재 구현과 목표

이미 `672277695e`에 다음 공개 API가 추가되어 있다.

- [GlobalSequenceConsumer](../../clients/src/main/java/org/apache/kafka/clients/consumer/GlobalSequenceConsumer.java):
  `fetch(topic, start, endExclusive, timeout)`, `wakeup`, `close` 인터페이스.
- [GlobalSequenceConsumerRecord](../../clients/src/main/java/org/apache/kafka/clients/consumer/GlobalSequenceConsumerRecord.java):
  global/physical offset, key/value, headers, timestamp, leader epoch를 담는 record.
- [GlobalSequenceConsumerRecords](../../clients/src/main/java/org/apache/kafka/clients/consumer/GlobalSequenceConsumerRecords.java):
  immutable record 목록과 `nextGlobalOffset`을 담는 page. C01에서 topic UUID, committed end, pending과 부분 오류를 보강했다.

C01의 공개 계약 테스트와 `GlobalSequenceTopicIdMismatchException`을 추가했고, C02~C05에서 전용 설정,
transport, decoder, `KafkaGlobalSequenceConsumer`의 fetch와 lifecycle 경로를 구현했다. C06에서
[GlobalSequenceConsumerExample](../../examples/src/main/java/kafka/examples/GlobalSequenceConsumerExample.java)과
사용 문서를 추가했다. 기존 [GlobalSequenceReadDemo](../../examples/src/main/java/kafka/examples/globalsequence/GlobalSequenceReadDemo.java)는
UUID와 wire mapping을 직접 다루는 raw protocol 진단 예제로 유지한다.

목표는 **그룹·읽기 position·offset commit을 관리하지 않는 동기식 한 페이지 client**다.
호출자가 매번 topic과 `[start, endExclusive)`를 지정하고, 다음 호출의 시작 offset도 직접 결정한다.
Client가 유지하는 연결·metadata cache·실행 중 요청은 읽기 position과 구분한다.

```java
try (GlobalSequenceConsumer<byte[], byte[]> consumer =
         new KafkaGlobalSequenceConsumer<>(properties)) {
    GlobalSequenceConsumerRecords<byte[], byte[]> page =
        consumer.fetch("globally-sequenced-topic", 0, 100, Duration.ofSeconds(10));
    // page.records() 처리 후 page.nextGlobalOffset()을 다음 호출에 사용
}
```

구현 클래스와 신규 설정은 C02~C04에서 아래 계약으로 구현했다. C01의 page 접근자와 expected UUID
overload도 구현했으며 기존 fetch signature와 record/page 생성자는 유지한다.
새 broker API나 global consumer group 프로토콜은 만들지 않는다.

## 2. 먼저 확정할 client 계약

### 2.1 한 페이지와 크기 제한

- `fetch` 한 번은 `FetchGlobalSequence`의 한 페이지를 반환한다. 실패한 시도의 재전송은 가능하지만,
  성공한 여러 페이지를 모아 요청 범위 전체를 반환하지 않는다. 다음 페이지를 미리 읽거나 저장하지 않는다.
- `0 <= start <= endExclusive`를 허용한다. 빈 범위도 UUID와 metadata가 확인된 빈 page를 반환한다.
  음수·역전 범위·잘못된 topic·음수 Duration은 네트워크 작업 전에 거절한다.
- 명시적 `Duration`은 metadata, 연결, 버전 협상, retry/backoff, fetch와 decode 경계 확인에 공통으로 적용한다.
  `Duration.ZERO`에서는 새로운 I/O를 시작하지 않고 TimeoutException을 반환한다.
- README의 size-bounded는 **원본 배치 bytes 기준의 페이지 제한**으로 설명한다. 서버 MaxBytes는 soft limit이라
  첫 완전 배치가 초과할 수 있다. 서버의 batch 8 MiB/전체 응답 16 MiB 상한과 별개다.
  압축 해제된 크기나 사용자 deserializer가 만드는 객체의 heap 크기를 같은 값으로 보장하지 않는다.
- `max.poll.records`처럼 결과를 임의의 record 수에서 자른 뒤 서버 cursor를 그대로 사용하는 방식은 쓰지 않는다.
  이는 아직 반환하지 않은 레코드를 건너뛸 수 있다.

### 2.2 Page 결과에 필요한 정보

현재 record 목록과 next offset만으로는 빈 결과의 원인과 안전한 재개 조건을 구분할 수 없다.
`GlobalSequenceConsumerRecords`에 아래를 추가했다. 기존 생성자는 유지하며,
C04의 실제 consumer는 snapshot 정보를 모두 채우는 새 생성자를 사용한다.
기존 생성자의 topicId는 `Uuid.ZERO_UUID`, committed end는
`UNKNOWN_COMMITTED_GLOBAL_END_OFFSET`(-1), pending은 false, error는 empty다.
기존 생성자가 입력 순서와 cursor를 검증하지 않는 동작도 유지한다.

| 추가 정보 | 이유 |
|---|---|
| `topicId()` | 다음 페이지나 저장한 cursor를 동일 이름의 새 토픽에 적용하지 않도록 식별 |
| `committedGlobalEndOffset()` | 현재 커밋된 index의 끝 확인. transaction이 모두 읽힌다는 뜻은 아님 |
| `transactionPending()` | READ_COMMITTED가 첫 미확정 global 구간에서 멈췄음을 표현 |
| `error()` (`Optional<ApiException>`) | 오류 전에 반환된 유효 prefix와 그 다음 지점의 오류를 함께 전달 |

인덱스의 물리 HW·leader epoch는 응답 검증과 진단에 사용한다. public page에 운영상 필요한 수준을
넘어 내부 라우팅 상태를 노출하지 않는다.

### 2.3 UUID와 topic 이름

기존 이름 기반 fetch는 한 호출이 시작할 때 확인한 UUID를 모든 재시도에서 고정한다.
Metadata refresh 중 같은 이름의 다른 UUID가 나타나면 기존 cursor를 새 UUID로 보내지 않는다.

호출 사이·client 재생성 뒤에도 identity를 보존할 수 있도록 아래 overload를 추가했다.

```java
fetch(String topic, Uuid expectedTopicId,
      long start, long endExclusive, Duration timeout)
```

기존 구현체의 소스·바이너리 호환성을 위해 새 overload는 default method다.
UUID를 검사할 수 없는 구현체에서는 기존 이름 기반 fetch를 호출하지 않고
UnsupportedOperationException으로 거절한다. Null/zero expected UUID도 먼저 거절한다.
C04의 consumer는 이 메서드를 override하여 metadata에서 expected UUID를 실제로 확인한다.

첫 페이지는 기존 이름 기반 메서드를 사용할 수 있다. 이후 페이지는 앞에서 받은 topicId를 전달한다.
Metadata와 expected UUID가 다르면 기대·실제 UUID를 포함한
`GlobalSequenceTopicIdMismatchException`으로 종료한다. 이 타입은 재시도 가능한 metadata 오류나
새 wire 오류가 아닌 KafkaException 하위의 client 오류다. Topic 이름과 두 UUID는 예외 직렬화 후에도 보존한다.
이름만 전달한 별도 호출들은 동일 이름의 토픽이 재생성되었는지 호출 간 보장할 수 없다는 점을 문서화한다.
따라서 체크포인트는 `(topic UUID, nextGlobalOffset)`이고, 예제도 두 번째 호출부터 expected UUID를 사용한다.

### 2.4 빈 결과·부분 오류·transaction

| 응답 상황 | Client 동작 |
|---|---|
| 정상 page | 원본 순서대로 역직렬화한 selected record와 서버 next cursor 반환 |
| 현재 index 끝/빈 범위 | 빈 page와 snapshot/cursor 반환. 숨은 tail 대기를 시작하지 않음 |
| 앞 transaction pending | 유효 prefix, pending=true, next cursor를 즉시 반환. transaction 종료를 내부에서 무한 대기하지 않음 |
| 전부 abort된 범위 | 빈 목록이어도 전진한 next cursor 반환. 번호를 당겨서 채우지 않음 |
| 오류 전까지 cursor가 전진한 page | prefix와 error를 함께 반환. record가 없어도 abort로 진전한 범위는 보존 |
| 진전 없는 재시도 가능 오류 | 동일 UUID·원래 범위로 남은 deadline 안에서 retry |
| 진전 없는 영구 오류/최종 timeout | 해당 Kafka 예외를 throw. 새로운 position을 저장하지 않음 |
| 손상·응답 식별자 불일치·역직렬화 실패 | 해당 호출의 page를 반환하지 않고 위치 정보를 담은 예외로 종료. 자동 skip 없음 |

Partial page를 반환한 뒤에는 client가 다음 범위를 자동 요청하지 않는다. 호출자가 prefix를 처리하고
error를 확인한 후 next cursor로 재시도 여부를 결정한다. RC 필터링으로 record 사이 global hole이
생기는 것은 정상이며, 반환 레코드들이 무조건 연속 번호여야 한다는 검증을 넣지 않는다.

### 2.5 스레드와 자원 수명

`fetch`와 `close`는 동일 client에서 동시에 호출할 수 없고 위반은 명시적으로 거절한다.
`wakeup`만 다른 스레드에서 허용하며 active/다음 fetch를 WakeupException으로 중단한다.
Close는 wakeup으로 중단하지 않고 멱등하게 네트워크·metadata·metrics·deserializer를 해제한다.
단일 호출의 timeout 뒤 남은 request/callback이 다음 호출 결과에 섞이거나 쌓이지 않도록 정리한다.

Deadline과 wakeup은 network poll뿐 아니라 batch/record decode 경계에서도 확인한다.
사용자 deserializer나 close callback 내부에서 블로킹하는 코드를 강제로 선점할 수는 없으므로,
이 제한은 close(Duration) 및 fetch timeout의 Javadoc에도 명시한다.

## 3. 커밋 단위 구현 순서

| 단계 | 범위 | 의존 | 완료 시점 | 상태 |
|---|---|---|---|---|
| C01 | 결과·identity·오류 계약 보강 | 기존 API | 공개 계약 확정 | 완료 |
| C02 | 전용 설정과 metadata/network 기반 | C01 | 실제 broker 연결·UUID 조회 | 완료 |
| C03 | wire page 검증과 record 역직렬화 | C01 | 순서·selected range·cursor 보존 | 완료 |
| C04 | 공개 consumer의 한 페이지 fetch와 retry | C02, C03 | 기본 사용 가능 | 완료 |
| C05 | wakeup·close·deadline·잔여 요청 수명 완성 | C04 | 인터페이스의 lifecycle 계약 완료 | 완료 |
| C06 | README의 consumer example와 사용 문서 | C05 | 명령으로 실행 가능 | 완료 |
| C07 | 실제 장애·transaction·보안 통합 검증 | C06 | consumer 1차 구현 완료 | 완료 |

각 단계는 필요한 테스트와 Javadoc을 함께 추가하여 하나의 커밋으로 저장한다.
완료 시 해당 단계의 상태·검증 결과와 커밋을 이 문서에 기록한다.

### C01. Public page와 identity 계약

상태: 완료 · 커밋: 본 커밋

커밋 제목: `feat: define global sequence consumer page and identity contracts`

- 기존 세 공개 타입을 기준으로 topicId/snapshot/pending/partial-error 정보를 보강한다.
- Expected UUID overload, identity 변경 예외 및 오류·timeout·threading Javadoc을 확정한다.
- Global record의 global offset과 physical partition/offset 의미를 유지한다.
  ConsumerRecords처럼 partition별 재정렬하는 구조로 변환하지 않는다.
- 기존 생성자/접근자의 소스 호환성, immutable 목록, 빈 pending/abort/error page 표현을 검사한다.

주요 위치: `clients/.../consumer/GlobalSequenceConsumer*.java`, 필요한 client 예외 타입과 테스트.
구현 내용:

- 새 page 생성자는 nonzero UUID와 nonnegative cursor/end를 요구한다. Global record offset은
  cursor/end 아래에서 엄격히 증가하되 abort로 인한 간격은 허용한다. 입력을 partition별로 재정렬하지 않는다.
- Pending과 error의 동시 표시는 거절하고, pending cursor는 committed end 아래여야 한다.
  빈 page의 start가 이미 committed end 뒤에 있는 경우 cursor를 뒤로 당기지 않는다.
- 목록은 방어 복사하여 변경을 막지만 key/value/header와 exception 객체까지 복사하지는 않는다.
- 테스트는 완료·pending·abort·부분 오류, 서로 다른 physical partition의 global 순서,
  Long.MAX_VALUE 경계, legacy 생성자/구현체 호환성과 UUID 오류 직렬화를 다룬다.
- Fetch deadline, zero timeout, no downgrade, wakeup 및 close의 동작은 인터페이스 Javadoc에
  확정했다. 실제 I/O·재시도·수명 구현은 C02~C05에서 이어서 검증한다.

완료 기준: 서버 상태를 records와 cursor에 손실 없이 표현하고, 호출자가 재개 결정을 할 수 있다.

검증: 새 공개 계약 테스트 20개와 기존 ConsumerRecords 회귀 5개, **총 25개 통과, 실패/skip 0개**.
Checkstyle main/test, SpotBugs main과 Javadoc 생성도 통과했다. Java 17에서 Gradle 8.14.1로
빌드했으며 네 공개 타입 모두 Java 11 class version 55로 생성됨을 확인했다.
실제 broker 연결과 consumer lifecycle 동작은 C02~C07의 범위이며 이번 검증에 포함하지 않는다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```


### C02. 설정·생성·Metadata 및 transport

상태: 완료 · 커밋: 본 커밋

커밋 제목: `feat: add global sequence consumer configuration and transport`

- `GlobalSequenceConsumerConfig`를 추가한다. `ConsumerConfig`를 통째로 사용해 group/assignment 설정을
  초기화하지 않고 common networking/security/metrics 및 deserializer 설정을 재사용한다.
- `bootstrap.servers`, client.id, DNS/reconnect/retry backoff, request timeout, SSL/SASL,
  key/value deserializer를 지원한다. Constructor에서 deserializer 인스턴스를 받는 방식도 정한다.
- `fetch.max.bytes`는 기본 1 MiB, 범위 1~16,646,144 bytes로 설정한다.
  일반 ConsumerConfig의 기본 50 MiB는 현재 global wire 상한보다 커서 그대로 가져오지 않는다.
- `global.sequence.fetch.max.batches`는 기본 100, 범위 1~1000을 제안한다.
  `isolation.level`은 기본 read_uncommitted이며 read_committed도 지원한다.
  group 가입·auto commit·assignment 관련 설정을 지원하지 않음을 명확히 진단한다.
- 기존 `NetworkClient`, `ClientUtils`, `ConsumerNetworkClient`의 poll/wakeup/future 경로를 재사용한다.
  ConsumerCoordinator, SubscriptionState, 일반 physical Fetcher는 생성하지 않는다.
- 요청 topic의 이름→UUID와 broker endpoint를 조회하는 metadata 계층을 추가한다.
  Topic auto-create를 끄고 권한·없는 topic·metadata refresh를 구분한다.
  Metadata cache는 읽기 position을 보유하지 않으며 여러 topic 조회 시 무제한으로 자라지 않도록 관리한다.
- Metadata/연결·ApiVersions까지만 내부 단위로 구성하고 생성 도중 실패 시 부분 자원을 회수한다.

검증: MockClient/MockTime 기반 bootstrap failover, metadata 갱신, UUID 확인, auth/config 오류,
보안 channel 설정 전달, constructor 실패 정리. Clients 모듈의 Java 11 호환성을 유지한다.

구현 내용:

- `GlobalSequenceConsumerConfig`는 network, DNS, timeout/backoff, metrics, SSL/SASL과
  optional deserializer class만 정의한다. ConsumerConfig를 상속하지 않으며 group, assignment,
  auto commit, offset reset, ordinary fetch 설정을 전달하면 설정 이름을 포함한 ConfigException을 낸다.
- Global fetch page는 기본 1 MiB/100 batches이고 각각 wire 상한 16,646,144 bytes와
  1~1000 범위를 검사한다. isolation은 read_uncommitted가 기본이며 read_committed를 허용한다.
- `GlobalSequenceConsumerTransport`는 `NetworkClient`, `ConsumerNetworkClient`, `ApiVersions`,
  metrics와 bootstrap/갱신 broker 목록만 소유한다. Coordinator, SubscriptionState와 일반 Fetcher는
  만들지 않으며 생성 중 주소 검증 등이 실패하면 이미 만든 metrics와 network 자원을 닫는다.
- Metadata resolver는 요청마다 auto-create=false인 단일 topic MetadataRequest를 보낸다.
  응답 broker 목록으로 다음 요청의 endpoint를 갱신하고 별도 topic cache를 쌓지 않는다.
  disconnect와 retriable metadata 오류는 같은 Timer 안에서 재시도한다.
- 응답 UUID가 없으면 구형 broker로 간주하고, expected UUID가 다르면 identity mismatch로 종료한다.
  없는 topic, topic authorization, invalid/non-retriable 오류를 서로 다른 예외로 보존한다.

완료 기준: group 상태 없이 실제 broker transport를 만들고, 하나의 호출 deadline 안에서 이름을
현재 topic UUID로 확인하여 C04 fetch에 넘길 수 있다.

검증 결과: C02 설정 12개, metadata 7개, transport 3개와 C01/ConsumerRecords 회귀를 합친
**총 47개가 통과했고 실패/skip은 0개**다. Checkstyle main/test, SpotBugs main과 Javadoc도 통과했다.
Java 17에서 Gradle 8.14.1로 빌드했으며 세 신규 main class가 Java 11 class version 55임을 확인했다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.GlobalSequenceConsumer*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```

### C03. Page decoder와 deserializer

상태: 완료 · 커밋: 본 커밋

커밋 제목: `feat: decode global sequence pages into consumer records`

- 응답 UUID, 범위, cursor, batch 수·크기, physical base/last/count와 global mapping의 산술 overflow를 검사한다.
- 응답 하나당 각 entry가 기대한 완전 배치인지, CRC·offset·count가 일치하는지 확인한다.
  READ_COMMITTED의 정상 global hole은 허용하고 overlap/역순/범위 밖 cursor는 거절한다.
- `global = globalBase + physicalOffset - physicalBase`로 계산하고 selected 범위에 해당하는 record만 반환한다.
- Key/value deserializer의 headers-aware 경로를 사용한다. Null, headers, timestamp/type,
  serialized size 및 원본 batch의 source leader epoch를 보존한다. Coordinator epoch와 혼동하지 않는다.
- 역직렬화 실패에는 topic UUID, physical 위치와 global 위치를 남긴다.
  객체 생성은 한 응답 범위로 한정하고, 실패한 page를 내부 position 없이 재요청할 수 있게 한다.
- 압축 배치를 순회하고 사용하는 iterator/buffer 자원을 확실히 해제한다. Decoder의 deadline/wakeup 확인
  지점을 주입 가능하게 만들어 C05에서 실제 lifecycle에 연결한다.

검증: 여러 파티션, 다중 record·압축 배치, 배치 중간 시작/종료, null/header/timestamp,
잘못된 CRC·truncated batch·잘못된 mapping/cursor, deser 실패, pending/abort의 빈 페이지.

구현 내용:

- `GlobalSequencePageDecoder`가 요청 topic/UUID/range와 응답 snapshot, cursor, partial error를 먼저
  대조한다. Snapshot 전 오류는 즉시 해당 broker 예외로 내고, 유효 prefix가 있는 오류는 page의
  `error()`로 보존한다.
- 각 mapping의 physical/global 산술 overflow, recordCount, selected intersection, committed end,
  batch 순서와 응답 batch/byte 상한을 검사한다. `maxBytes`보다 큰 첫 완전 배치는 허용하지만
  여러 batch가 soft limit를 넘는 응답은 거절한다.
- 각 entry는 magic v2의 비-control `MemoryRecords` 한 batch여야 한다. Encoded size, CRC 설정,
  physical base/last/count와 실제 record offset/count가 모두 일치해야 하며 truncated·다중 batch
  payload는 손상된 응답으로 처리한다.
- READ_UNCOMMITTED는 selected range와 cursor가 끊기지 않아야 한다. READ_COMMITTED는 aborted
  transaction으로 생긴 gap과 record 없는 cursor 전진을 허용하며 pending 상태를 별도로 보존한다.
- 선택 범위의 record만 headers-aware key/value deserializer에 전달한다. Null, timestamp/type,
  serialized size, headers, physical 위치와 source leader epoch를 `GlobalSequenceConsumerRecord`에
  보존한다. Deserialization 예외 메시지에는 topic UUID와 global/physical 위치를 함께 기록한다.
- 압축 iterator는 try-with-resources로 닫고 재사용 decompression buffer supplier도 decoder close에서
  해제한다. 주입된 boundary callback을 response, batch, record와 deserializer 사이에서 호출하여
  C05의 deadline/wakeup 검사에 연결할 수 있게 했다.

완료 기준: 한 wire response를 내부 position 변경 없이 완전히 검증하고, 성공한 경우에만 immutable
global-order page로 반환할 수 있다.

검증 결과: C03 decoder 9개와 C01~C02/ConsumerRecords 회귀를 합친 **총 56개가 통과했고
실패/skip은 0개**다. Checkstyle main/test, SpotBugs main과 Javadoc도 통과했다. Java 17에서
Gradle 8.14.1로 빌드했으며 신규 decoder가 Java 11 class version 55임을 확인했다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```

### C04. 한 페이지 fetch와 bounded retry

상태: 완료 · 커밋: 본 커밋

커밋 제목: `feat: implement stateless global sequence consumer fetch`

- 공개 `KafkaGlobalSequenceConsumer<K,V>`를 구성하고 C02 transport와 C03 decoder를 연결한다.
  Properties/Map과 명시적 deserializer constructor를 제공한다.
- `validate → resolve topic UUID → choose broker → FetchGlobalSequence → validate/decode → return`을 구현한다.
- Public global Fetch는 정상 broker 어디에나 전송 가능하므로 client가 index partition이나 source leader를
  계산하지 않는다. Broker 내부의 lookup/data RPC 라우팅을 재사용한다.
- 한 monotonic deadline에서 남은 시간을 계산한다. Wire TimeoutMs는 남은 시간과 30,000ms 상한을 적용하고,
  metadata 갱신·재연결·다른 broker 재시도 때 원래 deadline을 다시 시작하지 않는다.
- Disconnect, coordinator loading/이동, 일시적 metadata 오류, timeout과 overload에 대해 분류된 retry를 한다.
  UUID 변경·authorization·invalid input·unsupported version·원본 유실/손상은 무작정 반복하지 않는다.
- Broker throttle와 backoff를 적용한다. 반복되는 scan-limit 초과도 호출 deadline 안에서 끝나며,
  애플리케이션이 같은 cursor를 재시도하는 것만으로 해결되지 않을 수 있음을 설명한다.
- READ_COMMITTED는 v1을 요구하고 v0로 downgrade하지 않는다. RU는 지원 버전을 협상한다.
- Partial progress가 있는 page는 추가 재시도·페이지 결합 없이 오류와 함께 반환한다.

검증: MockTime의 단일 deadline, 동일 UUID/범위 유지, broker 교체, 버전 불일치,
성공 한 페이지만 반환, 빈 tip/pending/abort, partial error와 진전 없는 오류의 차이.

구현 내용:

- 공개 `KafkaGlobalSequenceConsumer`가 Properties/Map 설정과 설정 기반 또는 명시적으로 주입한
  deserializer를 받아 C02 transport와 C03 decoder를 연결한다. 생성 중 실패한 자원과 정상 종료 시
  transport, decoder buffer, deserializer의 소유권을 정리한다.
- `GlobalSequenceFetcher`는 최초 metadata에서 UUID를 확정하고 재시도마다 같은 UUID를 기대값으로
  다시 확인한다. 요청한 global 범위와 page 상한은 재시도 중 바꾸지 않는다.
- Metadata, broker 선택, network poll, retry/backoff와 decode 경계가 호출자가 만든 하나의 Timer를
  공유한다. Wire timeout은 남은 deadline과 30초 중 작은 값이며 재시도 때 감소한다.
- Disconnect와 진전 없는 retriable broker 오류는 broker throttle과 설정 backoff 중 큰 값을 적용해
  재시도한다. 정상/빈/pending page와 cursor가 전진한 partial error는 한 page로 즉시 반환하고,
  진전 없는 영구 오류와 손상된 응답은 예외로 끝낸다.
- READ_COMMITTED 요청 builder가 최소 v1을 요구하므로 v0 broker에서 격리 수준을 낮추지 않고
  `UnsupportedVersionException`으로 종료한다. READ_UNCOMMITTED는 지원 wire 버전을 협상한다.
- Public 입력은 I/O 전에 검증하며 zero timeout은 metadata 요청을 시작하지 않는다. 단일 스레드 guard,
  wakeup/close 경합과 종료한 요청 정리는 C05에서 완성했다.

완료 기준: Group/position 상태 없이 이름 또는 expected UUID와 명시 범위를 받아 실제 global fetch의
한 wire page를 원래 deadline 안에서 반환할 수 있다.

검증 결과: C04 fetcher 5개와 공개 consumer 4개, C01~C03/ConsumerRecords 회귀를 합친
**총 65개가 통과했고 실패/skip은 0개**다. Checkstyle main/test, SpotBugs main과 Javadoc도 통과했다.
Java 17에서 Gradle 8.14.1로 빌드했으며 두 신규 main class가 Java 11 class version 55임을 확인했다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.*GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```

### C05. Wakeup·close와 호출 종료 정리

상태: 완료 · 커밋: 본 커밋

커밋 제목: `fix: bound global sequence consumer request lifetimes`

- Metadata 대기, retry/backoff, fetch 응답 대기와 decode 중 wakeup을 같은 계약으로 연결한다.
- WakeupException/InterruptException/TimeoutException 후 실행 중 요청의 결과가 이후 fetch에 섞이지 않게 한다.
  한 번에 하나의 논리 fetch와 제한된 실제 in-flight 요청만 허용한다.
- Timeout 뒤 남은 queued/in-flight request를 취소·연결 정리하거나 완료까지 추적하는 정책을 구현한다.
  반복 timeout/wakeup으로 callbacks와 수신 buffers가 무한 증가하지 않음을 검사한다.
- 단일 스레드 사용 guard, close(Duration), 기본 close timeout 30초, close 후 fetch 거절,
  반복 close와 생성 실패 정리, network/metrics/deserializer의 정확히 한 번 close를 완성한다.
- Time/MockClient/주입 deserializer를 사용하여 임의 sleep에 의존하지 않는 경합 테스트를 추가한다.

완료 기준: 인터페이스에 선언된 모든 lifecycle 동작이 실제 구현과 일치한다.
Group heartbeat나 background prefetch thread는 필요하지 않다.

구현 내용:

- `GlobalSequenceRequestScope`가 한 논리 fetch에서 현재 전송한 metadata 또는 global fetch broker를
  하나만 추적한다. 정상 완료 시 즉시 해제하고 timeout, wakeup, interrupt와 예외 종료 시 해당 연결을
  끊은 뒤 callback을 wakeup 비활성 poll로 비워 다음 호출과 격리한다.
- Metadata와 fetch poll은 같은 request scope를 공유한다. 반복 timeout 후에도 unsent/in-flight 요청 수가
  0으로 돌아오며 늦은 응답은 다음 fetch 결과로 사용되지 않는다.
- Retry 및 broker throttle 대기는 network poll로 수행한다. 매 poll과 decode의 response/batch/record,
  deserializer 경계에서 wakeup과 thread interrupt를 확인하고 하나의 원래 Timer deadline을 유지한다.
- `KafkaGlobalSequenceConsumer`에 KafkaConsumer와 같은 thread owner/refcount guard를 추가했다.
  다른 thread의 fetch/close는 기다리지 않고 `ConcurrentModificationException`으로 거절하며 wakeup만
  lock 없이 허용한다.
- Close는 먼저 closed 상태를 확정하고 wakeup 전달을 비활성화한 뒤 decoder, caller/configured
  deserializer, network/metadata/metrics를 모두 닫는다. 반복 close는 자원을 다시 닫지 않으며
  close 이후 fetch는 `IllegalStateException`으로 거절한다.

검증 결과: C05에서 fetcher 5개, metadata 1개, 공개 consumer 1개를 추가했다. C01~C05와
ConsumerRecords 회귀를 합친 **총 72개가 통과했고 실패/skip은 0개**다. 반복 timeout 후 pending
request 0, wakeup 뒤 다음 fetch 격리, metadata/fetch poll, retry backoff, decode wakeup·interrupt,
close 경합과 정확히 한 번 자원 해제를 확인했다. Checkstyle main/test, SpotBugs main과 Javadoc도
통과했고 신규/변경 main class의 Java 11 class version 55를 확인했다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.*GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```

### C06. Consumer example와 문서 연결

상태: 완료 · 커밋: 본 커밋

커밋 제목: `feat: add global sequence consumer example`

- README에 적힌 `examples/src/main/java/kafka/examples/GlobalSequenceConsumerExample.java`를 추가한다.
- 기본 CLI를 그대로 지원한다: `<bootstrap> <topic-name> <start> <end-exclusive>`.
  선택적인 마지막 `client.properties`로 SSL/SASL, isolation과 page 크기를 지정한다.
- ByteArrayDeserializer와 Base64 출력을 사용하여 arbitrary binary 값을 손상 없이 표시한다.
  각 record의 global/physical 위치, page의 next cursor·committed end·pending·error를 출력한다.
- 첫 page의 committed end를 요청 end와 비교해 유한 읽기 끝을 정한다. 다음 호출부터 첫 UUID를 고정한다.
  Index 끝에 도달하면 종료하고, transaction pending이면 cursor를 출력하고 중단한다.
- Partial error에서는 앞 prefix와 다음 cursor를 출력한 뒤 오류로 종료한다.
  전부 abort되어 빈 목록인 경우에도 cursor가 전진했다면 다음 페이지를 읽는다.
- 기존 raw Lookup/Fetch demo는 protocol 진단용으로 유지하고, consumer 사용 경로와 역할을 구분한다.
- README의 현재 staged 문구를 기반으로 실제 class/명령과 일치하게 보완한다.
  [운영 가이드](global-sequence-operations.md)에 public consumer 사용법과 오류 처리 예제를 추가한다.

완료 기준: README에 적힌 명령이 이름 기반 consumer로 동작하며 앱이 위치를 직접 관리한다는 점이 드러난다.

구현 내용:

- CLI가 `<bootstrap> <topic-name> <start> <end-exclusive> [client.properties]`를 받고 properties의
  SASL/SSL, isolation, page batch/byte 제한을 public consumer에 전달한다. Bootstrap 인자는 파일의
  같은 설정보다 우선한다.
- Key/value는 `ByteArrayDeserializer`로 고정하고 record의 global/physical 위치, timestamp/type,
  source leader epoch와 Base64 key/value를 출력한다. Page마다 topic UUID, next cursor, committed end,
  pending과 partial error를 함께 출력한다.
- 첫 page의 UUID와 `min(requestedEnd, committedEnd)`를 실행 snapshot으로 고정한다. 이후 page는 expected
  UUID overload를 사용하며, abort로 record가 없어도 cursor가 전진하면 계속한다.
- End 도달과 pending에서는 정상 종료한다. Partial error는 처리 가능한 prefix와 cursor를 출력한 뒤
  예외로 종료하며, 진전 없는 page는 무한 반복하지 않고 실패한다.
- Examples README와 운영 가이드에서 public name-based consumer와 raw UUID/protocol 진단 도구를 구분하고,
  `(topic UUID, next global offset)` 체크포인트와 stdout의 비원자성을 문서화했다.

검증 결과: C06 예제 JAR 생성, Checkstyle main과 SpotBugs main이 통과했다. C01~C05 및
ConsumerRecords 회귀 **총 72개**와 clients Checkstyle main/test, SpotBugs main, Javadoc도 통과 상태를
재확인했다. 실제 broker에서 CLI 출력과 장애·transaction 동작을 확인하는 작업은 C07에 남아 있다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.*GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.ConsumerRecordsTest' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain :clients:javadoc \
  :examples:jar :examples:checkstyleMain :examples:spotbugsMain \
  -PmaxParallelForks=2 --max-workers=4 --continue --console=plain
```

### C07. 실제 broker 통합 검증과 완료 기록

상태: 완료 · 커밋: 본 커밋

커밋 제목: `test: verify global sequence consumer recovery and pagination`

- 기존 core KafkaClusterTestKit fixture를 활용해 실제 Produce→index commit→consumer fetch를 연결한다.
- 독립적으로 수집한 physical/index 로그의 매핑과 consumer 결과를 비교한다.
  다중 파티션, 작은 페이지, 배치 중간 범위, 압축·headers·null·timestamp를 확인한다.
- Source/index leader 이동, bootstrap broker 종료, 미커밋 index tail 뒤 복구,
  timeout 재호출에서 같은 cursor의 결과가 중복·누락 없이 이어지는지 검사한다.
- 열린 transaction, commit/abort, 같은 producer의 후속 commit, 모두 필터링된 페이지,
  broker 재시작 이후 두 격리 수준의 결과와 cursor를 검증한다.
- 원본 DeleteRecords의 partial error, 토픽 삭제·동일 이름 재생성의 expected UUID 거절을 확인한다.
- 사용자 READ 권한의 허용/거절과 보안 listener 연결을 검증하고,
  MockClient의 version negotiation 테스트로 v1 미지원 시 격리 downgrade를 차단한다.
- Network 관측/MockClient request 목록으로 JoinGroup/Heartbeat/OffsetCommit 및 일반 physical Fetch를
  consumer가 전송하지 않음을 확인한다.
- ConsumerExample을 실행해 출력/cursor와 종료 조건을 비교하고, 마지막으로 관련 회귀를 수행한다.

완료 기준: 기존 1~18번 계약을 유지하면서 README의 한 페이지 consumer 요구를 실제 client API로 충족한다.

구현 내용:

- `GlobalSequenceConsumerIntegrationTest`를 추가해 3 broker, RF=3/minISR=2의 실제 cluster에서 public
  consumer를 실행한다. Gzip physical batch의 null key/value, header, timestamp, source leader epoch와
  physical 위치를 실제 index log를 별도 KafkaConsumer로 읽어 만든 global oracle과 비교한다.
- `global.sequence.fetch.max.batches=1`로 배치 중간 범위와 두 page cursor를 확인한다. 첫 page 뒤 source와
  index leader를 옮기고 expected UUID로 이어 읽는다. 다른 broker를 bootstrap 목록에 남긴 채 한 broker를
  중단한 새 consumer도 전체 범위를 읽는다.
- Public fetch 전후 broker request metric에서 JoinGroup, Heartbeat, OffsetCommit이 증가하지 않고
  FetchGlobalSequence가 증가하는지 확인한다. 같은 fixture에서 실제 ConsumerExample을 실행해 Base64
  record 수, page 수, UUID, cursor, pending/error 출력을 검사한다.
- 열린 transaction 앞에서 READ_COMMITTED가 빈 pending page와 같은 cursor를 반환하는지 확인한다.
  Commit은 record를 반환하고 abort는 빈 page로 global hole을 한 칸 전진하며, 다음 committed batch는
  두 경우 모두 같은 expected UUID와 cursor로 이어진다. READ_UNCOMMITTED는 열린 두 batch를 모두 본다.
- 두 번째 global mapping의 source를 DeleteRecords로 제거하여 첫 record와 다음 cursor를 포함한
  `OffsetOutOfRangeException` partial page를 확인한다. 이어 토픽을 삭제·같은 이름으로 재생성하고 옛
  `(UUID, cursor)`가 `GlobalSequenceTopicIdMismatchException`으로 거절되는지 검사한다.
- 기존 fault/transaction/retention broker 테스트를 함께 실행하여 index HW timeout, source/index leader
  재시작과 미커밋 tail 복구를 회귀 검증한다. Lookup/Fetch API 테스트의 READ/CLUSTER_ACTION 권한 검사와
  client MockClient의 v1 negotiation·no-downgrade·timeout 정리도 같은 C07 검증 묶음에 포함한다.

검증 결과: 새 public consumer 실제 broker 시나리오 4개를 포함한 core 선별 회귀 31개와 C01~C06
client 회귀 72개, **총 103개가 통과했고 실패/skip은 0개**다. Clients/examples/core Checkstyle,
Clients/examples/core SpotBugs와 clients Javadoc도 통과했다. SASL/SSL 설정이 transport에 전달되는 경로는
client 단위 테스트에서 확인했으며, 별도의 보안 listener cluster는 이번 C07 fixture에서 실행하지 않았다.

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

## 4. 검증 명령과 범위

구현 후 다음처럼 실행한다. 테스트 클래스 이름은 C01~C07 구현 시 확정한다.

```sh
./gradlew :clients:test --tests 'org.apache.kafka.clients.consumer.*GlobalSequence*Test' \
  --tests 'org.apache.kafka.clients.consumer.internals.*GlobalSequence*Test' \
  :clients:checkstyleMain :clients:checkstyleTest :clients:spotbugsMain \
  :examples:jar :examples:checkstyleMain :examples:spotbugsMain \
  :core:test --tests 'kafka.server.GlobalSequenceConsumerIntegrationTest' \
  -PmaxParallelForks=2 --max-workers=4 --console=plain
```

신규 consumer 검증 후 [기존 회귀 명령](global-sequence-validation.md#3-재현-명령)도 실행한다.
현재 브랜치의 clients는 Java 11 target이므로 Java 17 record 등으로 public client 구현을 작성하지 않는다.
최종 결과에는 실제 실행한 테스트 수와 실패/skip, 미검증 범위를 기록한다.
C01의 실제 검증 결과는 해당 단계에 기록하고, 이후 consumer 통합 검증과 구분한다.

## 5. 범위 밖

Subscribe/assign/poll/seek/position, consumer group, offset commit, rebalance/heartbeat,
auto offset reset, background prefetch 및 여러 page를 모으는 전체 범위 fetch는 이 consumer의 범위가 아니다.
READ_COMMITTED를 다시 client에서 transaction index로 구현하거나 새 broker API를 만드는 작업도 필요 없다.

압축 해제·사용자 객체의 엄격한 heap 할당 예산, 장시간 성능 benchmark와 consumer group 통합은
필요 시 별도 계획으로 다룬다. 현재 계획의 메모리 보장은 wire/batch 상한과 단일 page 수명에 한정한다.
