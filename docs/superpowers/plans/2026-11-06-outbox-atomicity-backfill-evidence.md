# Outbox 원자성과 도입 전 누락 복구 검증

사용자가 요청한 보완은 원자적 저장 실패와 도입 전 누락 복구를 실행 근거로 설명하는 것입니다. 기존 그림8개와 RF3 측정값은 유지합니다. 운영 코드와 운영 DB는 변경하지 않습니다. 이미 승인된 게시 및 같은 URL의 갱신 범위에서 진행합니다.

## 설계와 검증 경계

별도 로컬 MySQL8.4/Kafka4.1.2 브로커3개, RF3/minISR2/acks=all을 사용합니다. Spring6.2.5의 프록시 기반 @Transactional/JdbcTemplate으로 결제 결과 저장과 Outbox INSERT를 같은 결제 DB 연결에 묶습니다. 주문 적용은 별도 DB 연결과 TX를 사용합니다. 실제 PG나 전체 운영 애플리케이션, MSK 장애를 검증했다고 표현하지 않습니다.

## Task 1: 원자적 저장 실패 검증

- 기존 모델에 Spring 프록시 기반 결과 저장 UseCase를 추가합니다. 같은 결제 행 잠금, 결과 유일성, 불변 snapshot과 고정 eventId를 유지합니다.
- 결제 결과 SQL 뒤 Outbox INSERT 전 실패, Outbox INSERT 뒤 commit 전 실패, commit 전 자식 JVM 강제 종료를 주입합니다. 각각 새 DB 연결에서 결제 상태 PENDING/결과0/Outbox0을 확인합니다.
- TX 경계를 의도적으로 분리한 대조에서는 결제만 APPROVED/Outbox0을 검출합니다. 같은 조건의 정상 TX가 이 불일치를 남기지 않는지 비교합니다.
- commit 직후 전송 전 자식 JVM 종료에서는 결과1/Outbox PENDING1이 남고, 재시작한 Relay가 주문을 한 번 반영하는지 확인합니다.

## Task 2: 도입 전 누락 복구 검증

- 불변 확정 결과는 있으나 Outbox가 없는 두 결제를 준비합니다. 하나는 주문 미반영, 하나는 HTTP로 이미 반영됐습니다.
- 저장된 확정 결과의 원주문/시도/순번/금액/통화와 snapshot에서만 Outbox를 재구성합니다. 현재 결제 상태로 승인 사실을 다시 만들지 않습니다.
- 동시 backfill과 재실행에서 동일 결과당 Outbox1행을 확인합니다. 이후 Relay/Consumer 반영으로 누락 주문만 변경되고 기존 주문의 변경 횟수는1회인지 확인합니다.
- 완료 뒤 반복 backfill/Relay가 신규 행과 record를 만들지 않는지 확인합니다. 이미 있는 Outbox의 payload 불일치와 원주문 연결 충돌은 덮어쓰지 않고 거절합니다.
- 모의 PG HTTP 호출 계수와 결제 승인 저장 횟수를 전후 비교합니다. 복구는 승인 API 호출 없이 저장된 결과만 사용합니다.

## Task 3: 본문과 게시

- section2와 section5에 실행 조건과 결과 표를 넣어 원자성 보장과 기존 누락 복구를 본문의 핵심 근거로 제시합니다.
- 필요한 복구 계약과 한계는 접힌 보충 자료에 둡니다. 기존6절/그림8개/측정 수치/외부 링크 정책을 유지합니다.
- 독립 리뷰, 수치와 HTML 구조 검증 뒤 게시합니다. Pages workflow 완료와 HTML/Markdown/CSS/PNG8개의 공개 바이트 일치를 확인합니다.

## 실행 결과

- Task1 완료: SQL 실행 뒤 예외2곳과 commit전/후 JVM halt를 확인했습니다. 체크포인트에서 실제행/동일연결/활성TX를 확인했고 새연결로 결과를 읽었습니다. commit후종료는 새Relay JVM이 저장된 envelope를 읽어 발행했습니다.
- Task2 완료: 8스레드16backfill 호출은 신규2/기존14, A적용0→1/B1→1, B동일snapshot Kafka2건, 복구중PG승인/조회추가0, 반복신규전송0, 원본없음/원주문충돌/기존payload충돌거절을 확인했습니다. 실패backfill rollback과 의도적분리TX대조도 확인했습니다.
- 최종suite exit0: 10시나리오와 요약1개, SQL체크포인트6개입니다. source/proof SHA를 최종sidecar와 대조했고 RF3/minISR2/ISR3 전체4토픽을 확인했습니다.
- Task3 본문 및 독립리뷰 완료: 기존6절/그림8개/지연수치유지, TOC포함details13개/링크/PNG크기/금지표현/공백검증통과. 별도Spring모델과 지정한두건복구라는범위를명시했습니다.
- 게시와공개파일일치는 push뒤 로컬 core-proof-deployment-verification.json에기록합니다.
