# Step 1. Producer / Consumer 기본 결과

> 참고: Step 2에서 기업 명세서를 반영해 `LogEvent` 스키마가 `service/level/message/host`에서 `device_id/source/event_time/collected_at/offset/raw(+level, message)`로, 메시지 키가 `service`에서 `device_id`로, `POST /api/logs` 요청 형식이 `{deviceId, raw}`로 바뀌었다. 아래 설명의 원리는 그대로이며, 필드 이름만 바뀐 것으로 읽으면 된다.

## 만든 것

Spring Boot 앱 하나에 프로듀서와 컨슈머를 함께 넣었다. 흐름은 다음과 같다.

```
POST /api/logs          ─┐
POST /api/logs/generate ─┴→ LogEventProducer.send() → [app-logs: 3 partitions] → LogEventConsumer.onLogEvent() → 콘솔 로그
                              (KafkaTemplate, key = service)                        (@KafkaListener, group = log-printer)
```

| 파일 | 역할 |
|---|---|
| `log/LogEvent.java` | 메시지 본체. Java record. `id`, `timestamp`, `service`, `level`, `message`, `host` |
| `config/KafkaTopicConfig.java` | `NewTopic` 빈으로 `app-logs` 토픽(파티션 3, 복제 1)을 기동 시 생성 |
| `producer/LogEventProducer.java` | `KafkaTemplate<String, LogEvent>`로 발행. 키는 service 이름. 전송 결과를 비동기로 로깅 |
| `producer/LogGenerator.java` | 무작위 로그 N건을 발행하고 모든 ack가 올 때까지 걸린 시간을 측정 |
| `consumer/LogEventConsumer.java` | `@KafkaListener`로 소비. 파티션/오프셋/키를 함께 출력 |
| `api/LogController.java` | REST 진입점 3개 (`POST /api/logs`, `POST /api/logs/generate`, `GET /api/logs/stats`) |
| `resources/application.properties` | 모든 Kafka 설정. 각 항목에 주석으로 의미를 적어두었다 |

## 설정에서 눈여겨볼 것

직렬화는 키를 `StringSerializer`, 값을 `JacksonJsonSerializer`로 두었다. 타입 헤더는 끄고(`spring.json.add.type.headers=false`) 컨슈머에서 `spring.json.value.default.type=com.practice.kafka.log.LogEvent`로 복원 타입을 고정했다. 이 결정의 이유는 `docs/concepts/02-spring-kafka.md`에 있다.

`acks=all`, `linger.ms=5`, `batch-size=16384`는 Step 1에서는 체감되지 않지만, `generate` 엔드포인트가 돌려주는 `elapsedMs`를 보면서 값을 바꿔보면 배치 효과를 볼 수 있다. `linger.ms=0`으로 두면 메시지마다 바로 전송해 요청 수가 늘고, `linger.ms=50`으로 두면 지연은 늘지만 처리량이 오른다.

`auto-offset-reset=earliest`는 "커밋된 오프셋이 없는 새 그룹"에만 적용된다. 한 번이라도 커밋한 그룹은 이 설정과 무관하게 커밋 지점부터 읽는다. 앱을 재시작했는데 이전 메시지를 다시 안 읽는 것은 이 때문이며, 정상이다.

## 실행 절차

```bash
# 0. 인프라 (Step 0에서 띄워두었다면 생략)
docker compose -f docker/docker-compose.yml up -d

# 1. 앱 실행 (최초 1회는 JDK 25 와 의존성을 내려받느라 수 분 걸린다)
./gradlew bootRun          # Windows: gradlew.bat bootRun
```

기동 로그에서 `KafkaAdmin`이 토픽을 만들었다는 메시지와, 컨슈머가 파티션 0, 1, 2를 할당받았다는 `partitions assigned: [app-logs-0, app-logs-1, app-logs-2]` 로그를 확인한다.

```bash
# 2. 로그 한 건 발행
curl -X POST http://localhost:8080/api/logs -H "Content-Type: application/json" \
  -d "{\"service\":\"order-api\",\"level\":\"ERROR\",\"message\":\"NPE at OrderService\",\"host\":\"host-a\"}"

# 3. 무작위 로그 1000건 발행 (응답의 elapsedMs = 1000건 전부 ack 받기까지 걸린 시간)
curl -X POST "http://localhost:8080/api/logs/generate?count=1000"

# 4. 이 인스턴스가 지금까지 소비한 건수
curl http://localhost:8080/api/logs/stats
```

IntelliJ나 VS Code를 쓴다면 `http/step1.http` 파일을 열어 같은 요청을 클릭으로 보낼 수 있다.

## 확인해야 할 것

콘솔에는 프로듀서의 DEBUG 로그(`발행 성공 ... partition=1 offset=37`)와 컨슈머의 INFO 로그(`[consume #38] partition=1 offset=37 key=order-api | ERROR order-api host-a - ...`)가 섞여 나온다. 같은 `service` 키의 메시지가 항상 같은 파티션 번호로 찍히는지 본다.

Kafka UI(http://localhost:8085)에서 세 군데를 본다. Topics → app-logs → Messages에서 JSON 본문과 파티션 분포를, Topics → app-logs → Consumers에서 `log-printer` 그룹이 3개 파티션을 모두 가졌고 lag이 0에 수렴하는지를, Brokers에서 브로커 1의 파티션 수가 늘어난 것을 확인한다.

## 테스트

```bash
./gradlew test
```

`@EmbeddedKafka`가 테스트 JVM 안에 KRaft 브로커를 띄우므로 docker 없이 돈다. `LogPipelineIntegrationTest`는 발행 → 소비가 실제로 이어지는지 확인한다.

## 배운 점

`send()`가 비동기라는 사실이 가장 중요하다. REST 응답이 202로 돌아왔다고 해서 브로커에 기록된 것이 아니다. 로그 파이프라인에서는 이 비동기성을 받아들이고 실패 콜백에서 대응하는 편이 처리량 면에서 옳지만, 유실이 허용되지 않는 데이터라면 Step 3의 멱등 프로듀서와 재시도 설정이 필요하다.

파티션 3개에 컨슈머 1개(concurrency 기본값 1)이므로 지금은 한 스레드가 세 파티션을 순회하며 읽는다. 병렬 처리는 Step 2에서 다룬다.

## 다음 단계로 넘어가기 전 실험 (선택)

`application.properties`에서 `spring.kafka.producer.properties[linger.ms]`를 0, 5, 50으로 바꿔가며 `generate?count=5000`의 `elapsedMs`를 비교해 본다. `acks`를 1로 바꿨을 때의 차이도 본다. 단일 브로커라 차이가 작을 수 있지만, 설정이 어디에 영향을 주는지 감을 잡는 데 의미가 있다.
