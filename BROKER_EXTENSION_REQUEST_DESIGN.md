# Broker Extension Request Design

## 1. 목적

Broker에 새로운 제어 요청 하나를 추가하고, 요청이 수행할 실제 로직은 broker interceptor가 주입할 수 있게 한다.

첫 번째 사용 사례는 monitoring log writer의 `flush()`와 divider 기록이지만, wire protocol과 Kafka request handler는 monitoring 구현을 알지 않는다. 이후 fault injection, 상태 snapshot, 실험 epoch 전환 같은 기능도 동일한 요청을 통해 별도 interceptor handler로 추가할 수 있어야 한다.

이 문서에서는 외부 API 이름으로 `BROKER_EXTENSION`을 사용한다. `INJECTABLE`이라는 이름은 실행 코드를 네트워크로 주입하는 것으로 오해될 수 있기 때문이다. 요청의 payload는 오직 등록된 handler가 해석하는 데이터이며, bytecode나 script를 실행하는 기능은 제공하지 않는다.

## 2. 목표와 비목표

### 목표

- 새 로직을 추가할 때 `KafkaApis`의 dispatch 코드를 계속 수정하지 않는다.
- 하나의 요청이 정확히 하나의 등록된 extension handler로 전달된다.
- 동기 및 비동기 로직을 모두 지원한다.
- handler 성공 또는 실패가 Kafka response에 반영된다.
- 인증, timeout, payload 크기 제한, lifecycle을 공통 계층에서 처리한다.
- 요청 payload와 handler API를 Kafka의 generated message class로부터 분리한다.
- 요청은 특정 broker에 대해 동작하며 controller forwarding을 사용하지 않는다.

### 비목표

- 네트워크를 통한 임의 코드, class, script 로딩
- 여러 broker에 대한 원자적 broadcast
- controller interceptor 실행
- extension 사이의 distributed transaction
- 최초 버전에서 handler discovery API 제공

## 3. 전체 구조

```text
Extension client
    |
    | BrokerExtensionRequest
    v
SocketServer / RequestChannel
    |
    v
KafkaApis.handleBrokerExtensionRequest
    |  - listener/config 검사
    |  - ACL 검사
    |  - payload 크기 검사 및 복사
    |  - deadline 생성
    v
BrokerExtensionRegistry.dispatch(command)
    |
    | target으로 정확히 하나의 handler 선택
    v
BrokerExtensionHandler.handle(command): CompletionStage[Result]
    |
    v
Kafka response callback
    |
    v
BrokerExtensionResponse
```

`beforeHandleRequest()`에서 extension 로직을 직접 실행하지 않는다. 그 hook 이후에도 `KafkaApis.handle()`이 실행되므로 응답 생성, 오류 전달, 인증 및 timeout 처리가 분산되기 때문이다.

## 4. Wire protocol

fork 전용 API key로 512를 사용한다. Kafka request header와 message generator는 signed short 범위의 sparse key를 지원하므로 512를 사용할 수 있다. 다만 이 번호는 Apache Kafka가 공식 배정한 번호가 아니므로, 향후 upstream과 충돌할 가능성이 있는 private protocol key라는 점을 문서화한다.

### BrokerExtensionRequest v0

```json
{
  "apiKey": 512,
  "type": "request",
  "listeners": ["broker"],
  "name": "BrokerExtensionRequest",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    {
      "name": "RequestId",
      "type": "uuid",
      "versions": "0+",
      "about": "Caller-generated identifier used by handlers for deduplication."
    },
    {
      "name": "Target",
      "type": "string",
      "versions": "0+",
      "about": "Registered extension handler name."
    },
    {
      "name": "Operation",
      "type": "string",
      "versions": "0+",
      "about": "Operation name interpreted by the selected handler."
    },
    {
      "name": "PayloadVersion",
      "type": "int16",
      "versions": "0+",
      "about": "Version of the target-specific payload schema."
    },
    {
      "name": "TimeoutMs",
      "type": "int32",
      "versions": "0+",
      "about": "Maximum time the broker should wait for the handler result."
    },
    {
      "name": "Payload",
      "type": "bytes",
      "versions": "0+",
      "nullableVersions": "0+",
      "zeroCopy": true,
      "about": "Opaque target-specific payload. This is data, not executable code."
    }
  ]
}
```

`Operation`을 protocol enum으로 만들지 않는다. 새 operation 추가가 Kafka protocol version 증가를 요구하지 않게 하기 위해서다. `PayloadVersion`은 Kafka API version과 별도로 target payload를 진화시키는 데 사용한다.

### BrokerExtensionResponse v0

```json
{
  "apiKey": 512,
  "type": "response",
  "name": "BrokerExtensionResponse",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    {
      "name": "ThrottleTimeMs",
      "type": "int32",
      "versions": "0+"
    },
    {
      "name": "ErrorCode",
      "type": "int16",
      "versions": "0+"
    },
    {
      "name": "ErrorMessage",
      "type": "string",
      "versions": "0+",
      "nullableVersions": "0+"
    },
    {
      "name": "PayloadVersion",
      "type": "int16",
      "versions": "0+"
    },
    {
      "name": "Payload",
      "type": "bytes",
      "versions": "0+",
      "nullableVersions": "0+"
    }
  ]
}
```

v0의 공통 오류 매핑은 다음과 같다.

| 상황 | Kafka error |
|---|---|
| 성공 | `NONE` |
| API 비활성화 또는 허용되지 않은 listener | `UNSUPPORTED_VERSION` |
| ACL 거부 | `CLUSTER_AUTHORIZATION_FAILED` |
| 알 수 없는 target/operation, 잘못된 payload | `INVALID_REQUEST` |
| payload version 미지원 | `UNSUPPORTED_VERSION` |
| handler timeout | `REQUEST_TIMED_OUT` |
| handler 예외 또는 commit 실패 | `UNKNOWN_SERVER_ERROR` |

## 5. 내부 extension API

Generated request/response 객체를 interceptor에 직접 넘기지 않는다. 다음과 같은 protocol-independent 객체로 변환한다.

```scala
final case class BrokerExtensionContext(
  brokerId: Int,
  connectionId: String,
  principalName: String,
  clientId: String,
  listenerName: String,
  deadlineNanos: Long
)

final case class BrokerExtensionCommand(
  requestId: Uuid,
  target: String,
  operation: String,
  payloadVersion: Short,
  payload: Array[Byte],
  context: BrokerExtensionContext
)

final case class BrokerExtensionResult(
  payloadVersion: Short = 0,
  payload: Array[Byte] = Array.emptyByteArray
)

trait BrokerExtensionHandler {
  def target: String

  def handle(
    command: BrokerExtensionCommand
  ): CompletionStage[BrokerExtensionResult]
}
```

Handler는 `CompletionStage`를 즉시 반환해야 한다. 파일 I/O, `MonitorLogWriter.flush()` 또는 대기 작업을 Kafka request handler thread에서 직접 실행하면 안 된다.

`IBrokerInterceptor`에는 handler 제공용 기본 메서드를 추가한다.

```scala
trait IBrokerInterceptor {
  // Existing lifecycle hooks...

  def extensionHandlers: Seq[BrokerExtensionHandler] = Seq.empty
}
```

이를 통해 기존 interceptor는 변경 없이 동작하고, 필요한 interceptor만 하나 이상의 target을 제공할 수 있다.

## 6. Registry와 dispatch 규칙

`BrokerInterceptors` 안에 직접 map을 넣기보다는 `BrokerExtensionRegistry`를 별도 클래스로 둔다. `BrokerInterceptors.init()` 시점에 모든 `extensionHandlers`를 모아 registry를 생성한다.

```scala
final class BrokerExtensionRegistry(
  handlers: Seq[BrokerExtensionHandler]
) {
  private val handlersByTarget: Map[String, BrokerExtensionHandler] = ...

  def dispatch(
    command: BrokerExtensionCommand
  ): CompletionStage[BrokerExtensionResult] = ...
}
```

등록 규칙:

- target은 `[a-z0-9][a-z0-9._-]{0,63}` 형식이다.
- target은 broker 안에서 유일해야 한다.
- 중복 target은 broker startup 실패로 처리한다.
- 요청 하나는 정확히 하나의 target으로 전달한다.
- interceptor 목록 전체에 broadcast하지 않는다.
- handler가 없는 target은 `INVALID_REQUEST`로 응답한다.
- registry는 초기화 이후 immutable이다.

Broadcast가 필요하면 client가 broker 목록을 조회해 각 broker로 요청을 전송한다. broker-local 상태를 controller로 forwarding하지 않는다.

## 7. Kafka request 처리

`KafkaApis` 생성자에 동일한 `BrokerExtensionRegistry` 또는 `BrokerInterceptors` 인스턴스를 주입한다. registry 직접 주입을 우선 권장한다. 이렇게 하면 `KafkaApis`가 interceptor lifecycle hook 전체를 알 필요가 없다.

개략적인 handler는 다음과 같다.

```scala
def handleBrokerExtensionRequest(
  request: RequestChannel.Request
): CompletableFuture[Unit] = {
  val extensionRequest = request.body[BrokerExtensionRequest]

  if (!extensionRequestEnabledFor(request.context.listenerName)) {
    requestHelper.sendMaybeThrottle(
      request,
      extensionRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception)
    )
    return CompletableFuture.completedFuture(())
  }

  if (!authHelper.authorize(
      request.context,
      ALTER,
      CLUSTER,
      CLUSTER_NAME)) {
    requestHelper.sendMaybeThrottle(
      request,
      extensionRequest.getErrorResponse(
        Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    return CompletableFuture.completedFuture(())
  }

  // RequestChannel releases its receive buffer after apis.handle returns.
  // An asynchronous handler must not retain a zero-copy ByteBuffer view.
  val payloadCopy = copyPayload(extensionRequest.data.payload)
  val command = toCommand(extensionRequest, request.context, payloadCopy)

  extensionRegistry.dispatch(command).handle[Unit] { (result, exception) =>
    val response = toResponse(result, exception)
    requestHelper.sendMaybeThrottle(request, response)
  }
}
```

`Payload`가 `zeroCopy`이므로 비동기 dispatch 전에 반드시 별도 byte array로 복사한다. 현재 `KafkaRequestHandler`는 `apis.handle()` 반환 후 request buffer를 release한다.

Top-level dispatch에는 다음 분기를 추가한다.

```scala
case ApiKeys.BROKER_EXTENSION =>
  handleBrokerExtensionRequest(request).exceptionally(handleError)
```

## 8. 실행, timeout과 backpressure

### 실행 모델

- Registry는 handler를 호출할 뿐 임의로 executor를 선택하지 않는다.
- 각 handler는 자신이 요구하는 threading 모델을 소유한다.
- 즉시 끝나는 검증이나 상태 조회는 completed future를 반환할 수 있다.
- blocking 작업은 interceptor 소유의 bounded executor에서 실행한다.
- interceptor shutdown 시 executor를 먼저 신규 작업 거부 상태로 만들고 진행 중 작업을 정리한다.

### Timeout

- broker 설정으로 최소/최대 timeout을 제한한다.
- 권장 기본 최대값은 30초다.
- `TimeoutMs <= 0` 또는 최대값 초과는 `INVALID_REQUEST`다.
- 공통 계층은 deadline 초과 시 `REQUEST_TIMED_OUT` 응답을 만든다.
- timeout response는 underlying side effect의 취소를 보장하지 않는다.
- handler는 `deadlineNanos`를 확인하고 가능한 경우 스스로 중단해야 한다.

### Backpressure

- target별 최대 in-flight 수를 둔다. 기본값은 1을 권장한다.
- 제한 초과는 무한 queueing 대신 즉시 `THROTTLING_QUOTA_EXCEEDED` 또는 `REQUEST_TIMED_OUT`으로 거부한다.
- request handler thread에서는 semaphore 획득을 기다리지 않는다.

## 9. 인증과 설정

이 API는 broker 내부 상태 변경이나 실험 기능 실행에 사용될 수 있으므로 기본 비활성화한다.

권장 broker 설정:

```properties
broker.extension.request.enabled=false
broker.extension.request.allowed.listeners=EXPERIMENT
broker.extension.request.max.payload.bytes=65536
broker.extension.request.max.timeout.ms=30000
broker.extension.request.max.in.flight.per.target=1
```

인증 정책:

- broker listener에서만 protocol을 노출한다.
- 설정으로 지정된 listener에서만 실행한다.
- 기본 ACL은 `ALTER` on `CLUSTER`를 요구한다.
- payload 내용은 일반 로그에 기록하지 않는다.
- audit log에는 principal, clientId, brokerId, requestId, target, operation, payload size, latency, 결과만 기록한다.

## 10. Retry와 idempotency

네트워크 timeout 뒤 client가 재시도하면 첫 요청이 이미 side effect를 수행했을 수 있다. 따라서 v0는 다음 계약을 사용한다.

- client는 모든 요청에 non-zero `RequestId`를 넣는다.
- registry는 자동 재실행 방지를 보장하지 않는다.
- non-idempotent operation은 handler가 bounded request-id cache를 사용해 중복을 제거한다.
- response가 유실될 수 있으므로 exactly-once 실행을 주장하지 않는다.
- `flush`는 반복 실행해도 안전하지만 `divider-and-flush`는 request-id 중복 제거가 필요하다.

추후 공통 dedup cache가 필요해지면 registry에 추가할 수 있지만, cache persistence가 없으면 broker restart를 넘는 exactly-once는 제공할 수 없다.

## 11. Monitoring interceptor 예시

Monitoring interceptor는 `monitor-log` target을 등록한다.

지원 operation:

- `flush`: 현재 writer queue를 write하고 commit한다.
- `divider-and-flush`: divider log를 submit하고 divider까지 flush한다.

```scala
final class MonitorLogExtensionHandler(
  monitorLogWriter: MonitorLogWriter,
  executor: ExecutorService
) extends BrokerExtensionHandler {

  override val target: String = "monitor-log"

  override def handle(
    command: BrokerExtensionCommand
  ): CompletionStage[BrokerExtensionResult] = {
    command.operation match {
      case "flush" =>
        CompletableFuture.supplyAsync(() => {
          if (!monitorLogWriter.flush())
            throw new IllegalStateException("monitor log commit failed")
          BrokerExtensionResult()
        }, executor)

      case "divider-and-flush" =>
        CompletableFuture.supplyAsync(() => {
          val divider = decodeDivider(command.payloadVersion, command.payload)
          submitDividerOnce(command.requestId, divider)
          if (!monitorLogWriter.flush())
            throw new IllegalStateException("monitor log commit failed")
          BrokerExtensionResult()
        }, executor)

      case _ =>
        CompletableFuture.failedFuture(
          new InvalidRequestException(
            s"Unknown monitor-log operation: ${command.operation}"))
    }
  }
}
```

현재 moniq `flush()`는 flush 요청을 writer thread로 전달하고, 당시 queue를 강제로 처리한 후 `writeStrategy.commit()`의 결과를 호출자에게 반환한다.

단, 현재 Kafka 쪽 `KafkaLogWriteStrategy.commit()`은 no-op이다. logger 호출 완료가 아니라 실제 파일 flush가 필요하다면 `FileMonitorLogWriteStrategy`와 같이 commit을 구현한 sink를 사용해야 한다.

또한 `flush()`는 queue에 제출된 로그에 대한 경계이지 처리 중인 Kafka request 전체에 대한 경계는 아니다. 이미 ingress log는 기록됐지만 response log가 아직 생성되지 않은 request가 있다면 divider 뒤에 response log가 기록될 수 있다. 완전한 request lifecycle 경계가 필요할 때만 별도의 epoch/in-flight drain 기능을 monitoring handler에 추가한다.

## 12. 변경 대상

### Protocol 및 client module

- `clients/src/main/resources/common/message/BrokerExtensionRequest.json`
- `clients/src/main/resources/common/message/BrokerExtensionResponse.json`
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`
- `clients/src/main/java/org/apache/kafka/common/requests/BrokerExtensionRequest.java`
- `clients/src/main/java/org/apache/kafka/common/requests/BrokerExtensionResponse.java`
- `clients/src/main/java/org/apache/kafka/common/requests/AbstractRequest.java`
- `clients/src/main/java/org/apache/kafka/common/requests/AbstractResponse.java`

### Server module

- `core/src/main/scala/kafka/interceptor/IBrokerInterceptor.scala`
- `core/src/main/scala/kafka/interceptor/BrokerInterceptors.scala`
- 신규 `BrokerExtensionHandler`, command/result/context, registry 타입
- `core/src/main/scala/kafka/server/KafkaApis.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/java/kafka/server/builders/KafkaApisBuilder.java`
- 필요 시 `server/src/main/java/org/apache/kafka/network/RequestConvertToJson.java`

Controller listener용 protocol이나 `ControllerApis`에는 추가하지 않는다.

## 13. 테스트 전략

### Registry unit test

- target dispatch 성공
- 알 수 없는 target 거부
- 중복 target 등록 시 startup 실패
- handler sync exception과 failed future 변환
- timeout 변환
- target별 in-flight 제한

### Protocol test

- v0 request/response serialize 및 parse round trip
- nullable payload
- 최대 payload 크기 경계
- API versions response에 broker listener에서만 노출

### KafkaApis unit test

- API disabled
- listener 제한
- ACL 허용/거부
- async 성공 응답
- handler 실패/timeout 응답
- request buffer release 이후에도 복사된 payload가 유효함
- throttle time 설정

### Monitoring handler test

- `flush`가 `MonitorLogWriter.flush()` 완료 후 응답
- commit false를 실패로 변환
- interrupted flush 처리
- divider가 queue의 선행 로그 뒤에 기록됨
- 동일 request ID 재요청 시 divider가 중복되지 않음

### Integration test

- 특정 broker에 요청하면 해당 broker handler만 실행됨
- handler가 없는 broker의 명확한 오류 응답
- 처리 중 broker shutdown 시 요청이 실패하고 hang하지 않음

## 14. 구현 순서

1. Internal command/result/handler 타입과 registry를 추가하고 unit test를 작성한다.
2. request/response schema, wrapper, parser, `ApiKeys`를 추가한다.
3. `KafkaApis`와 `BrokerServer`에 registry를 주입하고 인증/설정/timeout을 연결한다.
4. monitoring interceptor에 `monitor-log` handler를 추가한다.
5. protocol 및 `KafkaApis` unit test를 추가한다.
6. target broker를 지정할 수 있는 최소 client 또는 test client를 추가한다.
7. 통합 테스트 후 기본 비활성화 상태로 배포한다.

이 순서를 따르면 Kafka protocol을 추가하기 전에 extension dispatch의 동작과 오류 계약을 독립적으로 확정할 수 있다.

## 15. 커밋 계획

각 커밋은 독립적으로 빌드되고 해당 계층의 테스트를 통과해야 한다. 현재 작업 트리의 `.gitignore`와 `GLOBAL_SEQUENCE_PHYSICAL_RETRY_SCAN_CONSISTENCY.md`는 이 기능과 무관하므로 아래 커밋에 포함하지 않는다.

### 1. `docs: design broker extension request API`

포함 범위:

- 이 설계 문서 추가
- private API key 512, wire contract, dispatch 및 보안 정책 확정

검증:

- Markdown diff check
- 후속 구현과 명칭 및 필드가 일치하는지 리뷰

### 2. `build: update moniq for explicit writer flush`

포함 범위:

- `external/moniq` gitlink를 `c06a4c15ab7b2f4685b037f2c69c213918b06e7f`로 갱신
- 다른 상위 저장소 변경은 포함하지 않음

검증:

- `external/moniq` 전체 unit test
- `MonitorLogWriterFlushTest`
- 상위 Kafka 빌드에서 갱신된 moniq artifact가 사용되는지 확인

### 3. `feat(protocol): add broker extension request API`

포함 범위:

- API key 512의 request/response message schema
- `ApiKeys.BROKER_EXTENSION`
- `BrokerExtensionRequest`와 `BrokerExtensionResponse` wrapper
- `AbstractRequest`와 `AbstractResponse` parser 분기
- request/response JSON converter 분기
- protocol round-trip 및 API versions scope test

검증:

- message generation task
- clients request/response unit test
- broker listener에는 노출되고 controller listener에는 노출되지 않는지 검사
- sparse API key 512의 `forId`, parse 및 serialize 검사

### 4. `feat(core): add broker extension handler registry`

포함 범위:

- `BrokerExtensionContext`, `BrokerExtensionCommand`, `BrokerExtensionResult`
- `BrokerExtensionHandler`
- immutable `BrokerExtensionRegistry`
- `IBrokerInterceptor.extensionHandlers` 기본 구현
- `BrokerInterceptors` 초기화 시 handler 수집
- target 검증, 중복 검출, unknown target, exception normalization
- registry unit test

검증:

- 기존 interceptor가 수정 없이 초기화되는지 검사
- duplicate target이 broker startup 전에 실패하는지 검사
- sync exception과 failed future가 동일한 오류 계약으로 변환되는지 검사

### 5. `feat(server): dispatch broker extension requests`

포함 범위:

- `KafkaApis`에 `BROKER_EXTENSION` handler 추가
- `BrokerServer`와 `KafkaApisBuilder`에 registry 주입
- enable flag, listener allowlist, payload 및 timeout 제한 설정
- `ALTER` on `CLUSTER` ACL 검사
- 비동기 dispatch 전 payload 복사
- timeout, backpressure, response 및 audit/metric 처리
- `KafkaApis`와 configuration unit test

검증:

- disabled, listener 거부, ACL 거부 test
- async 성공, handler 실패, timeout, overload test
- request receive buffer release 이후에도 handler payload가 유효한지 검사
- 기존 request API regression test

### 6. `feat(interceptor): add monitor log extension handler`

포함 범위:

- `JsonBasedMonitorLoggingBrokerInterceptor`가 `monitor-log` target 제공
- `flush` operation
- `divider-and-flush` operation 및 divider payload codec
- blocking flush 전용 bounded executor와 shutdown lifecycle
- request ID 기반 divider 중복 방지
- monitoring handler unit test

검증:

- incomplete batch 강제 flush
- commit 성공/실패 응답
- interruption 및 shutdown 중 요청 처리
- divider ordering과 retry 중복 방지
- 현재 `KafkaLogWriteStrategy`와 실제 file strategy의 commit 의미를 각각 검사

### 7. `feat(tools): add broker extension request client`

포함 범위:

- target broker에 API key 512 요청을 보낼 최소 client 또는 CLI
- target, operation, payload, payload version, timeout 입력
- `flush`와 `divider-and-flush` 편의 명령
- request ID 자동 생성 및 응답 출력

검증:

- client request 생성 및 response parse test
- 잘못된 target/operation 오류 출력
- timeout과 broker connection failure 처리

### 8. `test: cover broker extension requests end to end`

포함 범위:

- 단일 broker 성공 integration test
- 다중 broker에서 target broker만 실행되는지 검사
- extension 미등록 broker의 오류 응답
- broker shutdown 및 timeout 시 hang 방지
- monitoring flush와 divider의 실제 출력 검증

검증:

- 관련 integration test suite
- clients/core/server 영향 범위의 최종 regression test

커밋 3부터 6까지는 각각 기능 코드와 해당 unit test를 같은 커밋에 넣는다. 마지막 커밋은 여러 계층을 실제 네트워크로 연결하는 integration test만 포함한다.
