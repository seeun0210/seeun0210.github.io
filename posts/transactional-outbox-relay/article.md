# 결제는 됐는데 판매자에게 주문이 보이지 않았다: Transactional Outbox와 Relay 개선

> 손님은 결제했다는데, 들어온 주문이 없어요.

결제 서비스에는 승인 성공 결과가 남아 있었지만 주문은 **결제 대기**였습니다. 판매자는 주문을 확인할 수 없었고, 고객에게 다시 결제하도록 안내하기도 어려웠습니다. 이미 승인된 결제를 한 번 더 만들 수 있기 때문입니다.

이 글에서는 **결제 결과 저장, 결과 전달, 주문 적용**을 나눠 문제를 살펴봅니다. Outbox가 보존하는 것과 폴링 조정으로 줄일 수 있는 대기를 확인하고, 콜백 Relay와 주문 적용 경로가 지켜야 할 계약까지 정리합니다.

## 1. 결제는 됐다는데, 주문은 어디 갔을까

처음에는 판매자 화면의 갱신 문제를 의심했습니다. 그런데 주문 조회 API에서도 해당 주문은 결제 대기였습니다. 화면을 새로고침해서 해결할 일이 아니었습니다.

같은 주문번호로 APM과 요청 로그를 따라갔습니다. 결제 서비스에는 PG 승인 성공과 결제 결과 저장 기록이 있었지만, 주문 서비스에는 그 결과를 반영한 기록이 없었습니다. 여기서 확인한 사실은 “주문이 결제 결과를 반영하지 못했다”는 것이었습니다. 이 사실만으로 Kafka가 유실했다고 원인을 확정할 수는 없었습니다. 응답 전달과 이벤트 발행, Consumer 적용 경계를 각각 확인해야 했습니다.

여러 축제의 여러 푸드트럭이 같은 플랫폼을 사용하며, 고객은 QR로 메뉴를 열어 주문합니다. 주문 서비스와 결제 서비스는 배포 단위가 분리돼 있습니다. 두 서비스는 같은 MySQL 인스턴스를 사용하지만 각자 소유한 테이블과 독립적인 트랜잭션을 관리합니다. 결제 서비스는 승인과 취소, 환불뿐 아니라 판매자별 정산과 지급대행도 담당합니다.

![주문과 결제 처리 흐름과 각 트랜잭션 경계](/assets/posts/transactional-outbox-relay/01-00-architecture-without-db-label-v2.png)

정상 경로는 주문 서비스 → 결제 서비스 → PG의 동기 HTTP 호출입니다. 결제 서비스가 승인 결과를 저장하고 응답하면 주문 서비스가 자기 TX에서 **결제 확인 후 판매자 수락 대기**를 기록합니다. 이후 판매자가 수락하면 조리 중으로 넘어갑니다.

결제 결과를 커밋한 직후 HTTP 응답이 유실되거나, 주문 서비스가 응답을 받고 자기 DB에 기록하기 전에 종료되면 두 상태가 갈라질 수 있습니다. 같은 MySQL을 사용해도 서비스 사이의 HTTP 호출이 하나의 로컬 트랜잭션이 되는 것은 아닙니다.

따라서 응답 타임아웃을 승인 실패로 확정하지 않습니다. 기존 `paymentAttemptId`의 결과를 확인하고, **이미 확정된 결과가 주문까지 도달할 경로**를 복구해야 합니다. HTTP가 정상일 때는 빠르게 반영하고, 응답을 놓쳤을 때도 결과를 다시 전달할 수 있어야 했습니다.

## 2. HTTP 응답을 놓쳐도 결제 결과를 다시 전달할 수 있어야 한다

기존 구현은 결제 결과를 커밋한 뒤 `@TransactionalEventListener(AFTER_COMMIT)`에서 Kafka로 이벤트를 보냈습니다. 핵심 경계만 남기면 다음과 같습니다.

```java
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
public void onPaymentConfirmed(PaymentConfirmed event) {
    kafkaTemplate.send("payment-events", event.paymentId(), event);
}
```

`AFTER_COMMIT`은 리스너를 실행할 시점을 정합니다. 결제 DB 커밋과 Kafka 기록을 원자적으로 묶어 주지는 않습니다. **커밋 후 리스너 실행 전**, 또는 **send 등록 후 브로커 기록 전**에 프로세스가 종료되면 결제 결과만 남을 수 있습니다. future를 수집하지 않으면 메서드가 정상 반환한 뒤의 전송 실패도 별도로 처리해야 합니다. [Spring의 트랜잭션 이벤트 실행 시점](https://docs.spring.io/spring-framework/reference/data-access/transaction/event.html)

![AFTER_COMMIT의 유실 구간](/assets/posts/transactional-outbox-relay/02-03-after-commit-gap.png)

`.get()`으로 ACK를 기다리면 이 문제가 해결될까요? 전송 결과는 확인할 수 있지만, 전송 전에 종료된 프로세스가 무엇을 보내려 했는지는 복원하지 못합니다. Kafka 전송이 실패해도 이미 끝난 결제 커밋은 되돌아가지 않습니다. 반대로 DB 커밋 전에 보내면 DB는 롤백됐는데 이벤트만 전달될 수 있습니다.

| 경계 | 중간 실패 뒤 남을 수 있는 상태 |
|---|---|
| DB 커밋 전 Kafka 전송 | 승인 결과는 롤백됐지만 이벤트는 전달됨 |
| DB 커밋 후 Kafka 전송 | 승인 결과는 남았지만 이벤트는 전달되지 않음 |
| 승인 결과와 Outbox를 같은 TX로 저장 | 커밋된 결과에 대응하는 발행 의도를 DB에서 다시 찾을 수 있음 |

발행 위치만 옮겨서는 두 저장소의 빈틈을 없앨 수 없었습니다. **재시작 후 다시 보낼 대상을 DB에 남기는 것**이 필요했습니다.

## 3. 결제 결과와 발행할 이벤트를 함께 저장해야 한다

결제 결과와 Outbox를 결제 서비스의 같은 로컬 TX에서 저장합니다. PG 호출은 이 TX 밖에서 수행하며, 여기에는 확인된 PG 결과를 전달합니다. 아래 코드는 승인 성공 결과를 기록하는 저장 경계를 축약한 것입니다.

```java
@Transactional
public ConfirmedPaymentResult recordResult(VerifiedPgResult pgResult) {
    var payment = payments.findByAttemptIdForUpdate(
        pgResult.paymentAttemptId());

    var existing = results.findByPgResultKey(pgResult.key());
    if (existing.isPresent()) {
        existing.get().verifySameFact(payment.id(), pgResult);
        return existing.get();
    }

    var result = payment.confirm(pgResult); // 검증과 순번 확정
    results.save(result);
    outbox.save(PendingPaymentEvent.from(result)); // ID와 payload 고정
    return result;
}
```

같은 원거래의 모든 결과 반영 경로는 동일한 결제 행을 잠급니다. PG 결과 키는 PG사와 상점 등 식별자 범위를 포함하고, DB에는 결과 키와 `(payment_id, event_sequence)`의 유일성 제약을 둡니다. 제약 충돌이 발생하면 실패한 TX를 롤백한 뒤 새 TX에서 기존 결과를 읽어 원거래와 금액이 같은지 확인합니다. 위 코드에서는 이 바깥 재조회 경로를 생략했습니다.

![결제 결과와 Outbox의 원자적 저장](/assets/posts/transactional-outbox-relay/03-04-atomic-outbox.png)

이제 승인 결과 저장과 Outbox INSERT가 모두 커밋되거나 모두 롤백됩니다. Outbox를 `REQUIRES_NEW`로 따로 저장하거나 `AFTER_COMMIT`에서 INSERT하면 이 경계가 다시 깨집니다. 주문 상태 변경과 PG 승인 자체는 이 로컬 TX에 포함되지 않습니다.

복구 책임은 중단 지점에 따라 달라집니다.

| 중단 지점 | 남는 상태와 복구 책임 |
|---|---|
| PG 승인 후, 결과와 Outbox 커밋 전 | PG와 DB가 다를 수 있습니다. 결제 서비스가 기존 시도의 거래 결과를 조회합니다. |
| 결과와 Outbox 커밋 후, Kafka 전송 전 | PENDING을 Relay가 다시 조회합니다. |
| Kafka ACK 후, PUBLISHED 커밋 전 | 같은 eventId가 재발행될 수 있습니다. 수신 측이 업무 중복을 막습니다. |
| PUBLISHED 커밋 후, 주문 반영 전 | 발행은 끝났습니다. Consumer lag와 보류 이벤트, 주문 적용 오류를 확인합니다. |

`PUBLISHED`가 뜻하는 것은 **Kafka 전송 완료를 DB에 기록했다**는 것입니다. 주문 적용 완료나 판매자 화면 노출까지 뜻하지 않습니다. Outbox와 Relay, Consumer가 각각 어떤 상태를 보장하는지 구분해야 복구할 위치도 찾을 수 있습니다.

결제 서비스 안의 1분 주기 대사 배치는 PG 결과가 미확정인 시도를 조회합니다. 결과를 새로 확정하면 같은 TX에서 결과와 Outbox를 저장합니다. **대사는 PG와 결제 DB의 불일치를, Outbox는 확정된 결과의 후속 전달을 담당**합니다. 이미 확정된 결과의 주문 반영을 다음 대사 시점까지 미룰 이유는 없습니다.

도입 전에 누락된 이벤트에는 Outbox 행이 없습니다. 이 경우에는 불변의 확정 내역과 주문 적용 이력을 대조하는 별도 복구가 필요합니다. 현재 결제 상태로 과거 승인 이벤트를 추측해 만들지 않고, 당시 결과를 재구성할 수 있는 대상만 원래 식별자로 연결해야 합니다.

### 이벤트는 현재 상태가 아니라 확정된 사실을 전달한다

```json
{
  "eventId": "event-101",
  "eventType": "PaymentSucceeded",
  "schemaVersion": 1,
  "eventSequence": 1,
  "occurredAt": "2026-11-06T03:00:00Z",
  "orderId": "order-101",
  "paymentAttemptId": "attempt-101",
  "paymentId": "payment-101",
  "amount": 18000,
  "currency": "KRW"
}
```

`eventId`는 이 사건의 식별자, `eventSequence`는 같은 결제의 결과 순서, `schemaVersion`은 payload 형식의 버전입니다. Relay는 저장된 ID와 payload를 그대로 재사용합니다. 재발행할 때 현재 결제를 다시 직렬화하면 같은 사건의 내용이 달라질 수 있습니다.

`payment-events` 토픽의 key는 `paymentId`로 통일합니다. 이벤트에는 적용에 필요한 식별자와 최소 단위 정수 금액, 통화를 담고 카드 번호나 PG 인증 정보는 넣지 않습니다. 승인 거절이 확정되면 `PaymentFailed`를, 타임아웃으로 불확실하면 실패 확정 대신 조회가 필요한 상태를 남깁니다.

식별자만 보내는 Zero Payload도 가능합니다. 다만 현재 상태 조회 API는 승인 뒤 환불된 결제를 돌려줄 수 있습니다. 당시의 불변 승인 내역을 조회하는 계약이 있어야 같은 사실을 전달할 수 있습니다. 여기서는 결제 조회 API의 가용성에 주문 반영을 다시 의존시키지 않도록 확정 정보를 담았습니다. [이벤트 알림과 데이터를 포함한 이벤트](https://martinfowler.com/articles/201701-event-driven.html)

## 4. 지연되어서 좋은 데이터는 없다.

Outbox는 전달할 일을 남깁니다. 언제 전달되기 시작할지까지 정해 주지는 않습니다. 주문이 몰릴 때는 결제 결과와 Outbox가 모두 남아 있는데도 판매자가 주문을 뒤늦게 확인하는 문제가 남았습니다.

이때는 PG 승인이 늦은 것과, **승인이 끝난 뒤 다음 전달 단계가 기다리는 것**을 분리해야 합니다.

| 구간 | 확인할 대상 |
|---|---|
| 승인 요청 → 결제 결과 확정 | PG 응답, HTTP와 호출 제한 대기 |
| 결제 커밋 → Relay send 시작 | 폴링 간격, 이전 회차 대기, 슬롯, 선행 이벤트 보류 |
| send 시작 → 전송 완료 | metadata와 버퍼, ACK, 내부 재시도 |
| Kafka 전달 → 주문 커밋 | Consumer 적체, 선행 결과 대기, 주문 DB |

`created_at`은 INSERT 시각이지 커밋 완료 시각은 아닙니다. 서비스 사이의 시간을 비교할 때는 관측 경계와 시계 차이도 맞춰야 합니다. HTTP로 이미 반영된 주문이나 ACK 후 결과 기록만 늦은 이벤트를 미반영 주문으로 세지 않습니다.

### 5초 fixedDelay는 매 5초 조회가 아니었다

기존 Relay는 최대 100건을 먼저 전송한 뒤 모든 future를 공통 마감까지 수집하고, 결과를 한 번에 기록했습니다.

```java
@Scheduled(fixedDelay = 5000)
public void relayOnce() {
    var events = reader.findDue(100);       // 짧은 읽기 TX
    var futures = sender.sendAll(events);  // 먼저 비동기 전송
    var outcomes = collectUntilSharedDeadline(futures);
    resultRecorder.recordBatch(outcomes);  // 별도 쓰기 TX
} // 메서드 반환 후 5초를 더 기다립니다.
```

실제 다음 조회까지는 **조회 + 전송 등록 + 배치 결과 대기 + 일괄 기록 + 5초**가 걸립니다. 처리 시간이 0이어도 한 회차 100건과 5초 대기라면 지속 발행량은 초당 20건을 넘기기 어렵습니다. 이는 코드에서 계산한 상한이며 측정한 최대 처리량은 아닙니다. [Spring fixedDelay의 실행 기준](https://docs.spring.io/spring-framework/reference/integration/scheduling.html)

먼저 폴링 사이의 빈 대기를 줄였습니다. HTTP 반영을 놓친 주문의 결제 확정 후 반영 지연은 운영 당시 회고로 약 5초에서 약 200ms로 줄었습니다. 이 근사값을 평균이나 p95로 부르지는 않습니다. 다음 측정에서는 같은 구간을 별도로 정의해 비교했습니다.

### 개선 전후에 같은 구간을 비교한다

측정은 **로컬 MySQL 8.4, Kafka 4.1.2 단일 브로커, 모의 PG HTTP** 환경에서 수행했습니다. 실제 PG나 운영 환경의 측정값은 아닙니다. 같은 JVM의 별도 HTTP 서버와 독립된 DB TX를 사용하고, `System.nanoTime()`으로 결제 커밋 완료 확인부터 주문 커밋 완료 확인까지를 기록했습니다.

조건마다 목표 유입 20건/초로 200건씩 3회 실행했습니다. 동일한 선전송과 일괄 기록 Relay에서 `fixedDelay`만 5초와 100ms로 바꿨고, 최초 실행도 각 delay 후였습니다. 아래는 **실행별 p95 / p99의 중앙값, 단위 ms**입니다. 각 실행에서 지연을 정렬한 뒤 `ceil(p × n)`번째 값을 골랐습니다. 전체 표본을 합친 분위수는 아니며, 이벤트 대상 40건의 p99는 해당 실행의 최대값입니다.

| 대상과 측정 구간 | 5초 폴링 | 100ms 폴링 |
|---|---:|---:|
| 정상 HTTP: 승인 처리 요청 → 주문 반영 | 113.6 / 182.7 | 77.5 / 93.3 |
| 응답 지연의 전체 주문: 결제 커밋 → 주문 반영 | 4,217.3 / 5,241.0 | 114.0 / 202.1 |
| 응답 지연의 이벤트 대상: 결제 커밋 → 주문 반영 | 5,241.0 / 5,349.5 | 202.1 / 215.9 |
| 응답 지연의 이벤트 대상: 승인 처리 요청 → 주문 반영 | 5,305.0 / 5,421.8 | 268.7 / 309.2 |
| PG 지연: 승인 처리 요청 → 주문 반영 | 8,598.9 / 9,154.6 | 4,247.8 / 4,281.0 |

응답 지연 조건에서는 매 5번째 요청의 결제 HTTP 응답을 결과 커밋 후 5초 늦춰 주문 서비스의 4초 타임아웃을 넘겼습니다. 실행당 200건 중 40건이 이벤트 반영 대상이었습니다. 이 대상의 반영 p95는 **5,241.0ms → 202.1ms**였습니다. 반복별 범위는 각각 4,933.6–5,278.9ms와 133.4–274.7ms입니다.

여기서 전체 주문 p95인 **114.0ms만 보면 이벤트 복구 경로의 p95 202.1ms를 읽을 수 없습니다.** HTTP가 먼저 반영하는 주문과 이벤트를 기다리는 주문은 다른 집단입니다. 그 비율이 달라지면 전달 경로가 같아도 전체 지표가 달라집니다. 장애 복구 경로의 품질을 보려면 그 경로에 의존한 주문을 따로 봐야 합니다.

PG 지연 조건은 모의 PG의 rolling 1초 호출이 9건을 넘으면 성공 응답을 3초 늦춘 조건입니다. 실제 나이스페이의 호출 한도나 지연 곡선이 아닙니다. 폴링을 줄여도 요청부터 반영까지 p95는 약 4.25초 남았습니다. **Relay는 결제 확정 이후의 전달 대기를 줄일 뿐 PG 대기를 없애지는 않습니다.** 정상 HTTP 경로의 작은 수치 차이도 폴링 변경만의 효과로 단정하지 않습니다.

### 완료된 주문과 아직 기다리는 주문을 나눠 본다

완료 주문의 분위수만으로는 지금 남아 있는 주문을 알 수 없습니다. 미반영 수와 가장 오래 기다린 시간도 함께 확인했습니다. 다음은 **20ms 간격으로 관측한 실행별 최대치의 중앙값**입니다.

| 미반영 지표 | 5초 폴링 | 100ms 폴링 |
|---|---:|---:|
| 응답 지연: 최대 미반영 수 | 21건 | 3건 |
| 응답 지연: 가장 오래 기다린 미반영 주문 | 5,343.8ms | 195.3ms |
| PG 지연: 최대 미반영 수 | 56건 | 5건 |
| PG 지연: 가장 오래 기다린 미반영 주문 | 5,183.6ms | 269.6ms |

세 조건의 총 18회 3,600건은 종료 후 Kafka 기록과 결제 결과, Outbox와 주문 DB를 대조했고 최종 미반영은 모두 0건이었습니다. 종료 시점의 0건이 측정 중에도 항상 0건이었다는 뜻은 아닙니다. 샘플링 사이의 최대치, 장기 운영 누락률과 장애 내구성도 이 결과로 보장하지 않습니다.

이 측정이 뒷받침하는 것은 **폴링 주기 단축으로 이벤트 반영 경로의 대기를 줄였다는 것**입니다. 다음 절의 콜백 전환이나 부분 환불, 다중 Relay의 검증 결과로 확대하지 않습니다. 판매자 화면 갱신과 실제 수락 시간도 포함하지 않았습니다.

## 5. 느린 한 건이 다른 결제의 발행을 막지 않도록 해야 한다

앞 절에서 수치로 확인한 범위는 폴링 주기 변경입니다. 여기부터는 배치 대기를 없애기 위한 **콜백 Relay의 설계 계약**을 다룹니다. 코드는 구현에 필요한 실행 경계를 축약한 것이며, 이 모델의 성능 수치를 앞 절의 결과로 대신하지 않습니다.

폴링 간격을 줄여도 배치 전체를 기다리는 구조는 남습니다. 기존 구현도 먼저 여러 `send()`를 시작했으므로 Kafka 전송이 한 건씩만 진행된 것은 아닙니다. **이미 전달된 이벤트는 Consumer가 바로 반영할 수 있었습니다.** 기다리는 것은 배치 뒤에 새로 쌓인 이벤트의 전송 시작이었습니다.

| 완료 수집 방식 | 다음 조회가 시작되는 조건 |
|---|---|
| 각 `send().get()` 후 다음 전송 | 이 루프에서는 한 건씩 ACK와 DB 기록을 마친 뒤, 선택한 배치가 끝나야 함 |
| 모두 send 후 future 수집 | 배치 전체의 결과 수집과 일괄 기록이 끝나야 함 |
| 슬롯을 제한한 완료 콜백 | 조회와 전송 등록 후 반환하고 다음 스케줄에서 빈 슬롯만 사용 |

바꿔야 할 것은 `.get()`이라는 메서드 자체보다 **회차의 종료 조건**이었습니다. 고정 작업자들이 `.get()`을 사용하는 제한된 동시 처리도 대안입니다. 여기서는 Kafka의 비동기 완료를 이어받는 콜백 구조로 스케줄러와 결과 기록을 분리합니다. 콜백이 보편적으로 처리량이 더 높다는 주장은 하지 않습니다.

![처리 슬롯 안에서 조회와 전송하고 완료 콜백으로 결과 기록](/assets/posts/transactional-outbox-relay/04-05-relay-flow.png)

```java
@Scheduled(fixedDelayString = "${outbox.relay.poll-delay-ms:100}")
public void relayOnce() {
    int free = inFlight.remainingCapacity();
    if (free == 0) return;

    var events = reader.findDueHeads(
        Math.min(fetchLimit, free), inFlight.eventIds());

    for (var event : events) {
        if (!inFlight.tryReserve(event.eventId())) continue;
        sender.sendAndTrack(event);
    }
} // ACK와 후속 DB 기록을 기다리지 않고 반환합니다.
```

단일 활성 스케줄러를 전제로 합니다. 읽기 TX를 끝낸 뒤 send 전에 ID와 슬롯을 예약하며, 다음 조회에서 처리 중 ID를 제외합니다. DB에는 아직 PENDING이어도 이 프로세스가 맡은 행입니다. 여러 프로세스가 동시에 발행하려면 DB 선점과 소유권, 만료 후 인계까지 별도로 설계해야 합니다.

조회 상한 100건과 전체 처리 한도는 다른 설정입니다. 전체 한도가 100건일 때 Kafka 결과 대기 60건과 DB 기록 대기 또는 진행 30건이 있다면 새로 맡을 수 있는 양은 10건입니다. 슬롯이 없으면 조회를 생략합니다. 슬롯 반환은 새 조회를 직접 호출하지 않고 다음 스케줄이 사용할 자리를 돌려줍니다.

슬롯은 Kafka 응답 대기부터 결과 기록 커밋까지 유지합니다. ACK만으로 반환하지 않고, DB 기록 실패 때는 같은 전송 결과의 기록만 재시도합니다. 재시도 가능한 최종 전송 실패는 다음 발행 시각을 커밋한 뒤 슬롯을 반환합니다.

완료 작업은 명시적인 `resultExecutor`에 넘기고, 큐와 DB 연결 수를 제한합니다. 건별 결과 커밋으로 DB 쓰기가 늘어나는 비용도 있습니다. 배치 대기가 메서드 반환에서 빠져도 DB 기록이 느리면 슬롯 반환과 다음 전송은 제한됩니다.

<details>
<summary>콜백의 DB 기록, 오류 회수와 종료 절차</summary>

```java
var sendFuture = kafkaTemplate.send(toRecord(event));
var recordFuture = sendFuture.whenCompleteAsync((result, sendError) -> {
    try {
        // 별도 빈의 프록시 호출이며 DB 커밋 후 반환합니다.
        resultRecorder.recordInNewTransaction(event, result, sendError);
        inFlight.complete(event.eventId());
    } catch (RuntimeException dbError) {
        // 슬롯을 유지하고 같은 전송 결과의 DB 기록만 재시도합니다.
        completionTracker.retryRecording(event, result, sendError, dbError);
    }
}, resultExecutor);
completionTracker.watch(event, sendFuture, recordFuture);
```

DB 작업을 Kafka 전송 완료 스레드에서 실행하지 않도록 명시적인 `resultExecutor`를 사용합니다. 원본 future와 결과 기록 future는 따로 추적합니다. 후속 future는 원래 전송 오류도 유지하므로, 예외 완료만으로 DB 기록 실패라고 판정할 수 없습니다. **send의 동기 예외와 실행기 작업 거부도 completionTracker가 회수하는 경로**가 필요합니다. 위 코드는 이 복구 구현을 생략한 연결 구조입니다. [Spring Kafka의 비동기 전송 결과](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html)

![스케줄러와 Kafka 완료 스레드, DB 작업 실행기 사이의 슬롯 수명](/assets/posts/transactional-outbox-relay/05-11-callback-slot-lifecycle.png)

ACK를 받았더라도 DB 기록 전에는 슬롯을 돌려주지 않습니다. 기록 실패 시 슬롯을 먼저 반환하면 같은 PENDING을 다시 읽을 수 있기 때문입니다. 메모리에 전송 결과를 보존하고 DB 기록만 재시도합니다. 재시도 가능한 전송 실패는 다음 발행 시각을 DB에 커밋한 뒤 슬롯을 반환합니다.

이 선택에는 비용도 있습니다. 배치당 한 번이던 결과 커밋이 건별로 나뉘고, 결과 실행기와 재시도 작업을 관리해야 합니다. 큐와 DB 연결 수도 제한해야 합니다. 배치 ACK 대기가 메서드 반환에서 빠져도, **DB 기록이 느리면 슬롯 반환이 늦어져 다음 전송 역시 제한**됩니다.

![실패 단계별 복구 경로와 정상 종료 순서](/assets/posts/transactional-outbox-relay/06-12-callback-recovery-shutdown.png)

종료 순서는 신규 조회와 전송 중지 → 진행 중 전송과 후속 DB 기록 대기 → 결과 실행기 종료입니다. 실행기를 먼저 닫으면 뒤늦게 도착한 ACK를 기록할 곳이 없어집니다. `producer.flush()`만으로 외부 DB 작업 완료를 판단하지 않습니다. 유예 시간 안에 끝나지 않은 행은 DB에 남겨 다음 프로세스가 같은 ID로 재발행할 수 있어야 합니다.

</details>

### Producer 내부 재시도와 Outbox 재발행을 구분한다

```properties
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
retry.backoff.ms=100
retry.backoff.max.ms=1000
delivery.timeout.ms=30000
request.timeout.ms=2000
linger.ms=5
max.block.ms=5000
```

`delivery.timeout.ms=30000`은 한 전송 시도에 내부 재시도 기회를 주되 슬롯을 계속 점유하는 예산을 제한하려는 값입니다. DB 기록 완료까지의 상한은 아닙니다. 비동기 `send()`도 metadata와 버퍼를 기다리므로 `max.block.ms`는 5초로 별도 제한합니다. 그래도 조회 시작 간격에는 실제 조회와 send 등록 시간이 더해집니다. 이 값들을 최적값이나 모든 환경의 권장값으로 보지는 않습니다.

Kafka의 멱등성은 같은 producer 상태에서 보내는 배치의 sequence로 내부 재시도를 판별합니다. Relay가 같은 Outbox 행으로 **새 send를 호출하면 새 전송**입니다. ACK 후 DB 기록 전에 종료된 이벤트가 다시 보내져도, Kafka가 payload의 eventId를 읽어 두 호출을 합치지는 않습니다. [Kafka Producer의 배치 sequence 할당](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java)

| 재시도 계층 | 유지해야 할 것 | 완료 기준 |
|---|---|---|
| Producer 내부 재시도 | 클라이언트가 관리하는 전송 상태 | 전송 future 완료 |
| Relay 재발행 | 저장된 eventId와 동일 payload | 결과 기록 TX 커밋 |
| 주문 재적용 | paymentId와 eventSequence의 같은 확정 결과 | 주문 변경과 적용 이력의 TX 커밋 |

<details>
<summary>재시도 백오프와 오류 격리의 상세 정책</summary>

한 번의 전송이 실패할 때마다 바로 Outbox 행을 갱신하고 새로 보내면, 짧은 Kafka 장애에도 DB 쓰기와 재발행이 늘어납니다. 짧은 오류는 Producer 내부에서 재시도하고, 그 전송 시도까지 실패한 경우에 Outbox의 다음 발행 시각을 미루도록 나눴습니다.

Kafka 응답이 늦어지면 ACK 대기와 내부 재시도가 길어지고, 전송이 끝나지 않은 이벤트가 처리 슬롯을 계속 차지합니다. `send()` 자체가 metadata나 버퍼를 기다리는 시간도 따로 제한해야 합니다. 이에 따라 재시도 간격과 한 전송의 완료 예산, 전송 등록 대기를 나눠 설정했습니다.

| 설정 | 선택한 값 | 선택 근거 |
|---|---|---|
| `retry.backoff.ms` / `retry.backoff.max.ms` | 100 / 1000ms | 일시 오류의 첫 재시도는 빠르게 시작하되, 실패가 이어질 때는 간격을 늘려 같은 장애에 요청을 계속 몰아넣지 않도록 합니다. |
| `delivery.timeout.ms` | 30초 | 기본 120초보다 한 전송의 재시도 예산을 줄입니다. 최종 실패를 기록한 뒤의 재전달은 Outbox 백오프로 넘깁니다. |
| `max.block.ms` | 5초 | 기본 60초보다 전송 등록 대기를 줄입니다. 단일 스케줄러가 metadata 조회와 버퍼 확보 때문에 오래 멈추는 시간을 제한합니다. |

재시도 관련 설정을 추리면 다음과 같습니다. `request.timeout.ms=2000`, `linger.ms=5`와 함께 사용해, `delivery.timeout.ms`가 두 값의 합보다 커야 하는 조건도 맞췄습니다.

```properties
retry.backoff.ms=100
retry.backoff.max.ms=1000
delivery.timeout.ms=30000
max.block.ms=5000
```

이때 **30초는 주문 반영의 목표 시간이 아니라, 장애 중 한 전송 시도에 허용하는 시간**입니다. 정상 전송은 ACK를 받는 즉시 결과를 기록하고 슬롯을 반환합니다. 전송 타임아웃 뒤에도 Kafka에 기록됐을 가능성은 남으므로, 다시 발행할 때는 같은 eventId를 유지하고 Consumer가 중복을 막습니다. 설정 변경 후에는 재시도 건수뿐 아니라 처리 슬롯 점유, 가장 오래된 PENDING의 대기 시간, 주문 반영 지연을 함께 봤습니다. [Kafka 프로듀서 설정](https://kafka.apache.org/41/configuration/producer-configs/)

전송 시도가 최종 실패하면 Relay는 재시도 가능한 행의 `attempt_count`와 `next_attempt_at`을 결과 기록 TX에서 갱신합니다. 횟수는 Outbox의 새 전송 시도 기준이며 Kafka 내부 재시도 횟수와 다릅니다. BLOCKED는 잘못된 payload처럼 자동 재시도로 해결되지 않는 오류를 격리할 때 사용합니다. Kafka 장애나 단순 관측 타임아웃을 이유로 결제 결과 이벤트를 삭제하지 않습니다.

기본 조회 조건은 `status = 'PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP(6)`이며, 현재 처리 중인 ID도 제외합니다. 신규 행은 생성 즉시 발행 대상이 되도록 초기 시각을 넣습니다. 다만 이 조건만으로는 같은 결제의 이벤트 순서를 지킬 수 없으므로, 선행 미발행 이벤트가 있는지도 함께 확인해야 합니다.

백오프 정책 예시로 기본 간격을 1초, 2초, 4초처럼 늘리되 최대 30초로 제한하고, 각 간격의 50–100% 범위에서 무작위로 대기 시간을 정할 수 있습니다. 같은 시점에 실패한 이벤트가 함께 몰려 재전송되는 것을 줄이기 위한 jitter입니다. 이벤트마다 Relay 스레드를 재우지 않고 다음 시각을 DB에 남깁니다. DB 기록 실패의 재시도와 Kafka 발행 실패의 재시도는 구분합니다.

행별 백오프만으로 Kafka 전체 장애 중 신규 이벤트의 발행 시도까지 억제되지는 않습니다. 광범위한 연결 실패에는 신규 전송을 잠시 줄이는 정책도 함께 고려합니다. 서킷브레이커를 추가하더라도 차단된 이벤트는 Outbox에 보존하고 복구 후 다시 전달해야 합니다. 오래 기다리는 이벤트와 반복 실패, 결과 기록 적체도 관측합니다.

</details>

### CDC로 전환하기 전에 현재 병목부터 구분해야 한다

CDC를 사용해도 결과와 Outbox를 같은 TX에 저장하는 구조는 유지합니다. 달라지는 것은 발행 경로입니다. Debezium이 MySQL binlog의 Outbox INSERT를 읽으면 주기적 SELECT의 발견 대기를 줄일 수 있지만 Kafka ACK와 Consumer, 주문 DB의 지연까지 사라지지는 않습니다.

먼저 개선한 것은 폴링의 빈 대기였고, 콜백 설계는 배치 전체 대기로 다음 조회가 막히는 경계를 다룹니다. 이 둘을 해결할 여지가 있는 상황에서 발행 경로 전체를 CDC로 옮기기보다 현재 경로부터 정리했습니다.

표준 Outbox Event Router는 INSERT 중심입니다. 현재 Relay의 `next_attempt_at` UPDATE를 재발행 지시로 해석하지 않으며 DB의 PUBLISHED도 자동으로 갱신하지 않습니다. 전환 시에는 행별 재시도와 수동 재발행, 발행 상태 조회와 정리 기준을 다시 맞춰야 합니다. [Outbox Event Router의 동작 계약](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)

또한 CDC는 저장된 binlog 위치나 GTID에서 재개하므로, 중단 중 필요한 로그가 삭제되면 재스냅샷 등 복구가 필요합니다. 커넥터의 중단 허용 시간, binlog 보존과 Outbox 정리 정책을 함께 정해야 합니다. 조회 비용이나 발견 대기가 계속 지배적일 때 다시 비교할 수 있습니다. [MySQL 커넥터의 재시작과 복구](https://debezium.io/documentation/reference/stable/connectors/mysql.html)

## 6. 중복과 순서 역전을 구분해 주문에 반영해야 한다

이벤트를 여러 번 전달할 수 있게 했다면 수신 측의 적용 계약도 필요합니다. 다음은 HTTP와 Kafka가 공유해야 할 트랜잭션 경계와 순서 처리 조건입니다.

정상 HTTP 응답과 Kafka 이벤트가 모두 도착할 수 있습니다. 이때 eventId만으로 중복을 막기는 어렵습니다. HTTP에도 `paymentId`, `paymentAttemptId`, `eventSequence`를 전달하고 **같은 확정 결과를 적용하는 공통 경로**를 사용합니다.

같은 결제의 적용을 잠금으로 직렬화하고, `(payment_id, event_sequence)` 적용 이력 INSERT와 주문 변경, 마지막 적용 순번 갱신을 같은 주문 TX에 넣습니다. 존재 확인과 주문 변경을 따로 커밋하면 두 경로가 모두 “아직 적용하지 않았다”고 판단할 수 있습니다. 유일성 충돌 뒤에는 롤백과 재조회로 이미 적용된 같은 결과인지 확인합니다. 같은 결과 키의 금액이나 원주문이 다르면 중복이 아니라 충돌입니다.

재발행과 수동 복구가 가능한 동안 같은 결과임을 판별할 기준도 보존해야 합니다. PUBLISHED 행 정리와 주문 적용 이력의 보존을 같은 주기로 맞추지는 않습니다.

![주문 업무 적용과 inbox의 트랜잭션 경계](/assets/posts/transactional-outbox-relay/07-08-consumer-transaction.png)

### key가 같아도 Relay가 늦게 제출하면 순서는 바뀐다

승인 이벤트 A가 백오프 중일 때 취소 이벤트 B를 먼저 보내면, `payment-events`의 key가 같아도 Kafka에는 B → A로 들어갑니다. 결제 서비스에는 실제 승인 내역이 있으므로, 주문 서비스가 A를 받기 전에 PG 취소가 끝나는 상황도 가능합니다. A와 B는 다른 eventId라 중복 검사로 막을 문제도 아닙니다.

따라서 `eventSequence`는 주문 반영용 결과에 대해 결제별로 연속 증가시키고, 결과와 순번, Outbox를 같은 결제 TX에서 저장합니다. 정산과 지급대행의 다른 이벤트를 같은 순번에 섞지 않습니다. **선행 미발행 결과가 있으면 같은 결제의 후속 발행만 보류**하고 다른 결제는 계속 처리합니다.

<details>
<summary>같은 결제의 후속 발행을 제한하는 조회 조건</summary>

```sql
SELECT e.*
FROM payment_outbox e
WHERE e.status = 'PENDING'
  AND e.next_attempt_at <= CURRENT_TIMESTAMP(6)
  AND NOT EXISTS (
      SELECT 1
      FROM payment_outbox previous
      WHERE previous.payment_id = e.payment_id
        AND previous.event_sequence < e.event_sequence
        AND previous.status IN ('PENDING', 'BLOCKED')
  )
ORDER BY e.next_attempt_at, e.id
LIMIT 100;
```

후보에서는 현재 프로세스의 처리 중 ID도 제외합니다. 다만 선행 행을 찾는 서브쿼리에는 재시도 시각이나 처리 중 ID 제외를 적용하지 않습니다. 이미 send했거나 백오프 중인 선행 행도 후속 발행을 막아야 합니다. ACK 후 PUBLISHED 커밋이 끝나야 다음 순번이 후보가 되며, BLOCKED도 후속 발행을 막습니다.

이 조회는 단일 활성 스케줄러 조건입니다. 인덱스는 후보 검색의 `(status, next_attempt_at, id)`와 선행 검색의 `(payment_id, event_sequence)` 등을 실제 데이터의 실행 계획으로 확인해야 합니다. PENDING과 BLOCKED는 정리 대상에서 제외합니다.

</details>

### 선행 결과가 없으면 받은 사실과 적용 완료를 구분한다

기존 메시지나 별도 복구 경로로 취소 2가 승인 1보다 먼저 도착할 수 있습니다. 이때 취소를 버리거나 같은 파티션에서 승인 도착까지 붙잡고 있으면 복구가 어려워집니다. 뒤에 있는 승인을 읽지 못할 수도 있기 때문입니다.

| 마지막 적용 순번 | 도착한 결과 | 처리 |
|---|---|---|
| 0 | 취소 2 | payload와 ID를 inbox에 WAITING_FOR_PREDECESSOR로 보존 |
| 0 | 승인 1, 보류된 취소 2가 있음 | 연속 결과를 적용하고 커밋 시점에는 취소까지 반영 |
| 1 | HTTP로 이미 반영한 승인 1 | 같은 결과인지 대조하고 업무 변경 반복 없음 |

보류 기록이 DB에 커밋되면 Kafka offset을 넘길 수 있습니다. 그 대신 DB를 재조회하는 복구 작업자가 이후 책임을 맡아야 하며 재시작 후에도 재개돼야 합니다. DB 저장이 실패하면 offset을 넘기지 않습니다. `eventId`를 받았다는 표시와 업무 적용 완료를 같은 상태로 취급하지 않습니다. [Spring Kafka의 offset 커밋 방식](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html)

이미 취소를 요청하거나 완료한 주문에 늦은 승인이 와도 수락 대기로 되돌리지 않습니다. 승인 사실은 이력에 남기고 주문의 현재 업무 상태와 함께 판단합니다. 순번만 가장 큰 값으로 덮어쓰면 필요한 승인과 환불 사실이 빠질 수 있습니다.

<details>
<summary>부분 취소와 환불: 같은 결제에서도 별도 결과를 식별해야 한다</summary>

부분 취소가 가능하다면 paymentId만 보고 “이미 환불된 결제”로 처리할 수 없습니다. 같은 원승인에 첫 번째 5,000원 환불과 두 번째 3,000원 환불이 각각 발생할 수 있기 때문입니다. **원주문은 orderId, 원승인 결제는 paymentId, 이번 취소와 환불 작업은 refundId로 구분**합니다. PG의 원승인 거래 ID와 이번 취소 거래 ID도 별도로 연결합니다.

다음은 18,000원 결제에서 5,000원 부분 취소가 완료된 내부 이벤트입니다. PaymentCancelled는 전체 주문 취소라는 뜻이 아니라 PG 승인 취소가 완료됐다는 결제 사실을 나타냅니다.

```json
{
  "eventId": "event-102",
  "eventType": "PaymentCancelled",
  "schemaVersion": 1,
  "eventSequence": 2,
  "occurredAt": "2026-11-06T03:02:00Z",
  "orderId": "order-101",
  "paymentId": "payment-101",
  "paymentAttemptId": "attempt-101",
  "originalPgTransactionId": "pg-approval-101",
  "refundId": "refund-201",
  "pgCancelledTransactionId": "pg-cancel-201",
  "orderAdjustmentId": "adjustment-201",
  "approvedAmount": 18000,
  "refundAmount": 5000,
  "refundedAmountTotal": 5000,
  "remainingAmount": 13000,
  "currency": "KRW"
}
```

`refundAmount`는 이번 건의 금액이며, `refundedAmountTotal`과 `remainingAmount`는 이 순번의 결과를 확정한 시점의 누적 금액과 잔액입니다. 누적 금액을 다시 더하지 않습니다. 이후 3,000원이 추가 환불되면 새로운 refundId와 다음 순번으로 이번 환불 금액 3,000원과 누적 환불액 8,000원, 잔액 10,000원을 전달합니다. 같은 refundId의 재수신은 중복이지만, 다른 refundId의 부분 환불은 별도로 반영해야 합니다. 금액 검증은 같은 결제에 대한 확정 내역을 기준으로 직렬화하고, 환불 결과와 잔액, 순번, Outbox를 같은 결제 TX에서 기록합니다.

주문 서비스는 취소할 주문 항목 ID와 수량, 할인 배분을 반영한 환불 금액을 불변의 주문 변경 내역으로 저장하고, 그 식별자인 `orderAdjustmentId`를 결제 서비스에 전달합니다. 이벤트를 받은 주문 서비스는 자기 DB의 변경 내역과 원주문과 원결제를 대조합니다. 결제 서비스가 주문 테이블을 직접 읽지 않아도 어떤 메뉴 몇 개에 대한 취소인지 연결할 수 있습니다. 주문 항목 취소 없는 금액 보상은 수량 취소와 구분합니다.

NICEPAY 취소 API도 원승인의 `tid`와 취소 결과의 `cancelledTid`를 구분하고, `balanceAmt`로 취소 후 잔액을 제공합니다. 부분 취소 요청의 PG `orderId`는 요청별 고유값을 사용하도록 되어 있으므로, 내부 원주문 orderId를 모든 부분 취소에 그대로 재사용하지 않고 refundId에 연결한 요청 식별자로 매핑합니다. 응답을 놓쳤다면 기존 환불 요청을 조회하고 대조해 복구하며 새 환불로 무조건 재호출하지 않습니다. [NICEPAY 취소와 환불 API](https://github.com/nicepayments/nicepay-manual/blob/main/api/cancel.md)

주문 적용 시 orderId와 paymentId, paymentAttemptId, orderAdjustmentId의 연관 관계를 확인합니다. 같은 refundId의 재수신은 중복이지만 다른 부분 환불을 같은 작업으로 합치지 않습니다. 일부 메뉴만 취소됐다면 해당 수량과 환불액만 반영하고 나머지 조리와 제공은 유지합니다. 재고 복구는 금액으로 수량을 역산하지 않고 주문 변경의 대상 수량과 조리 상태, 예약 처리 이력으로 판단합니다. 조리 완료 후 금액 보상은 판매 가능한 수량 복구와 구분합니다.

</details>

## 7. 관측하기

운영에서 확인할 기준은 **결제가 끝난 주문이 어느 경계에 남아 있는가**입니다. `orderId → paymentAttemptId → paymentId와 eventSequence → eventId`로 원거래와 전달, 적용 이력을 연결합니다. 백오피스도 이 구분을 드러내도록 구성합니다.

| 보이는 상태 | 확인할 대상 |
|---|---|
| PG 결과 확인 중 | 기존 결제 시도와 거래 결과 조회 |
| 결제 확정 후 주문 미반영 | HTTP와 이벤트의 적용 이력, 가장 오래 기다린 주문 |
| 선행 결과 대기 | 빠진 순번, 보류 시간, 불변 결과와의 대조 |
| 결제 확인 후 수락 대기 | 시스템 반영은 완료, 판매자 응답 대기 |
| 조리 중, 준비 완료, 픽업 완료 | 수락 이후 조리와 제공 흐름 |

![결제와 주문 반영 상태를 함께 확인하는 백오피스 구성](/assets/posts/transactional-outbox-relay/08-backoffice-payment-result-overview-v9.png)

사이드 프로젝트에서 축제 계약을 확보한 뒤에는 행사 중 결제와 주문이 안정적으로 이어져야 했습니다. 별도 운영 인력이 충분하지 않은 상황에서는 문의마다 로그를 처음부터 찾기보다, 어느 구간을 확인해야 하는지 한 화면에서 구분할 수 있는 것이 중요했습니다.

완료 주문의 경로별 반영 지연, 지금 남은 미반영 수와 나이, Outbox 대기, Kafka lag, 결과 기록 적체를 함께 봅니다. HTTP로 이미 적용된 주문은 Outbox가 PENDING이어도 미반영 주문이 아닙니다. 판매자 수락 대기도 Relay 지연에 포함하지 않습니다. 대시보드의 승인 건수와 PUBLISHED 건수를 맞추는 것보다 **주문 DB에 어떤 결제 결과가 실제로 적용됐는지**가 기준입니다.

이후 주문 서비스와 결제 서비스의 DB까지 분리되고 백오피스의 통합 조회 비용이 커지면, 이벤트로 갱신하는 Materialized View 역할의 읽기 모델도 검토할 수 있겠습니다.

Outbox로 남기는 것은 재전달할 근거입니다. 폴링 조정으로 줄이는 것은 전달 시작을 기다리는 시간입니다. HTTP와 이벤트의 공통 적용 TX로 막아야 하는 것은 같은 결과의 중복 변경입니다. 이 세 경계를 연결해야 결제 서비스의 성공 기록이 판매자가 처리할 수 있는 주문으로 이어집니다.
