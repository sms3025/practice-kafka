# 04. Consumer 깊게: 리스너 컨테이너, 그룹, 오프셋, 리밸런스

## 1. @KafkaListener 뒤에 있는 것

```java
@KafkaListener(topics = "app-logs", groupId = "log-printer")
public void onLogEvent(@Payload LogEvent event, ...) { ... }
```

앱이 뜨면 Spring은 이 메서드마다 `ConcurrentMessageListenerContainer`를 만든다. 컨테이너는 `concurrency` 설정(기본 1)만큼 내부 컨슈머(`KafkaConsumer` 인스턴스 + 전용 스레드)를 만들고, 각 스레드는 아래 루프를 앱이 끝날 때까지 돈다.

```
while (running) {
    ConsumerRecords records = consumer.poll(Duration.ofMillis(pollTimeout));  // 기본 최대 500건
    for (record : records) {
        onLogEvent(역직렬화(record.value()), ...);     // 우리 메서드
    }
    consumer.commitSync();     // ack 모드 BATCH: 묶음 처리 후 커밋
}
```

세 가지가 중요하다. 첫째, **우리 메서드는 컨슈머 스레드에서 호출**된다. REST 스레드가 아니다. 그래서 `LogEventConsumer`의 카운터가 `AtomicLong`이다. 둘째, 메서드가 오래 걸리면 다음 `poll()`이 늦어진다. `max.poll.interval.ms`(기본 5분)를 넘기면 브로커가 이 컨슈머를 죽은 것으로 보고 파티션을 뺏어간다. 셋째, 메서드가 예외를 던지면 커밋하지 않는다(4절).

## 2. 컨슈머 그룹: 나눠 읽기와 전부 읽기

`groupId`는 컨슈머의 소속이다. 브로커는 그룹별로 "어느 파티션을 누가 읽는지"와 "어디까지 읽었는지"를 관리한다.

```
토픽 app-logs: [p0] [p1] [p2]

그룹 log-printer (컨슈머 1개)      →  c1 이 p0, p1, p2 전부
그룹 log-printer (컨슈머 3개)      →  c1:p0  c2:p1  c3:p2       나눠 읽기 (병렬)
그룹 log-printer (컨슈머 4개)      →  c1:p0  c2:p1  c3:p2  c4: 없음   파티션보다 많으면 논다
그룹 clickhouse-sink (별도 그룹)   →  위와 무관하게 p0, p1, p2 전부 다시 읽음   전부 읽기 (브로드캐스트)
```

규칙은 두 줄이다. **같은 그룹 안에서 한 파티션은 한 컨슈머에만** 할당된다. **그룹이 다르면 서로 독립**이다. 이 두 줄로 "처리량을 늘리려면 같은 그룹에 컨슈머를 추가(파티션 수까지)", "같은 데이터를 다른 용도로 또 쓰려면 새 그룹"이 유도된다.

`concurrency`는 한 프로세스 안에 컨슈머를 여러 개 두는 것이다. `spring.kafka.listener.concurrency=3`으로 두면 스레드 3개가 파티션 하나씩 맡는다. 프로세스를 여러 개 띄우는 것과 그룹 입장에서는 같다.

## 3. 오프셋: "어디까지 읽었는가"

파티션의 각 메시지는 0부터 증가하는 오프셋을 갖는다. 컨슈머 그룹은 파티션마다 **커밋된 오프셋**(다음에 읽을 위치)을 브로커의 내부 토픽 `__consumer_offsets`에 저장한다. 메시지를 "소비했다"는 것은 사실 "오프셋을 커밋했다"는 뜻이고, 브로커는 메시지를 지우지 않는다(보존 기간이 지날 때까지).

```
파티션 p1:  [4819][4820][4821][4822][4823][4824]  ← 마지막 오프셋(log end offset) 4824
                              ▲
                      커밋된 오프셋 4821 → lag = 4824 - 4821 = 3 (아직 안 읽은 메시지 수)
```

**lag**은 Kafka 운영에서 가장 중요한 지표다. lag이 계속 늘면 컨슈머가 생산 속도를 못 따라가는 것이고, 컨슈머(또는 파티션)를 늘려야 한다. Kafka UI → Consumers → 그룹 이름에서 파티션별 lag을 본다.

### auto-offset-reset

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

이 설정은 **커밋된 오프셋이 없을 때만** 적용된다. 새 groupId로 처음 붙거나, 커밋된 오프셋이 보존 기간을 넘어 삭제된 경우다. `earliest`는 파티션의 처음부터, `latest`는 지금부터다. 한 번이라도 커밋한 그룹은 이 설정과 무관하게 커밋 지점부터 읽는다. "앱을 재시작했는데 이전 메시지를 다시 안 읽는다"는 이것 때문이며 정상이다. 처음부터 다시 읽고 싶으면 groupId를 바꾸거나 Kafka UI에서 그룹의 오프셋을 리셋한다(앱을 끈 상태에서만 가능하다).

### ack 모드와 커밋 시점

```properties
# spring.kafka.listener.ack-mode=batch   (기본값)
```

| 모드 | 커밋 시점 | 의미 |
|---|---|---|
| `BATCH` (기본) | poll 로 받은 묶음을 전부 처리한 뒤 | 처리 중 죽으면 묶음 전체 재처리 |
| `RECORD` | 레코드 하나 처리할 때마다 | 재처리 범위 최소, 커밋 비용 최대 |
| `MANUAL` / `MANUAL_IMMEDIATE` | 메서드 안에서 `ack.acknowledge()` 호출 시 | 저장소에 쓴 것을 확인한 뒤 커밋하는 식으로 통제 |

어느 모드든 "처리 → 커밋" 순서이므로, 처리 후 커밋 전에 죽으면 재시작 시 같은 메시지를 다시 받는다. 이것이 **at-least-once**이고 Kafka 컨슈머의 기본 보장이다. 컨슈머 로직은 같은 메시지를 두 번 받아도 결과가 같도록(멱등하게) 짜야 한다. 로그 적재라면 "같은 멱등키는 덮어쓴다"가 그 방법이다.

## 4. 예외가 나면

`onLogEvent`가 예외를 던지면 Spring의 `DefaultErrorHandler`가 개입한다. 기본 동작은 같은 레코드를 최대 10회(백오프 0ms) 재시도하고, 그래도 실패하면 로그를 남기고 **건너뛴다**(오프셋 커밋). 즉 기본값은 "포이즌 필 한 건 때문에 파티션 전체가 멈추지 않게" 되어 있지만 그 한 건은 버려진다. 로그 수집에서는 버리는 대신 Dead Letter Topic(`app-logs.DLT`)으로 보내 나중에 살펴보는 것이 맞다. Step 5에서 `DeadLetterPublishingRecoverer`로 바꾼다.

역직렬화 실패는 조금 다르다. `poll()` 단계에서 터지므로 우리 메서드까지 오지도 않고, 같은 레코드에서 계속 실패해 파티션이 막힌다. 그래서 운영에서는 `ErrorHandlingDeserializer`로 감싸 실패한 레코드를 null 또는 DLT로 보낸다. 이것도 Step 5의 항목이다.

실험: `onLogEvent`에 `if (event.level() != null && event.level().equals("ERROR")) throw new RuntimeException("test");`를 넣고 `simulate?lines=200`을 보낸 뒤 콘솔을 본다. 같은 메시지가 10번 재시도되는 로그와 그 뒤 건너뛰는 로그를 확인한다. 실험 후 원복한다.

## 5. 리밸런스: 그룹 구성이 바뀔 때

그룹에 컨슈머가 추가되거나(앱을 하나 더 띄움), 빠지거나(앱 종료, 장애, `max.poll.interval.ms` 초과), 토픽 파티션 수가 바뀌면 브로커의 그룹 코디네이터가 파티션을 다시 나눈다. 이것이 리밸런스다. 기본 프로토콜(eager)에서는 리밸런스 동안 그룹의 모든 컨슈머가 잠시 소비를 멈춘다. 몇백 ms에서 몇 초다. 자주 일어나면 처리량이 눈에 띄게 떨어지므로, 컨슈머를 자주 재시작하거나 처리 시간이 들쭉날쭉해 타임아웃이 나는 상황을 피해야 한다.

Kafka 4.x에는 새 그룹 프로토콜(KIP-848, `group.protocol=consumer`)이 있어 리밸런스가 점진적으로 일어나고 멈춤이 거의 없다. 이 프로젝트는 아직 기본(classic)을 쓰며 Step 3에서 둘을 비교해 본다.

실험(Step 3의 핵심): 앱을 `--server.port=8081`로 하나 더 띄운다. 첫 앱 콘솔에 `partitions revoked`와 `partitions assigned`가 찍히며 세 파티션이 두 앱에 나뉜다. Kafka UI → Consumers → log-printer 에서 멤버 2개와 각자의 파티션을 본다. 두 번째 앱을 끄면 다시 첫 앱이 세 파티션을 모두 가져간다.

## 6. 파티션 수를 어떻게 정하는가

파티션 수는 **그 토픽을 동시에 읽을 수 있는 컨슈머의 최대 수**다. 1,000대 장비 × 60줄/s = 6만 줄/s를 적재기 한 개가 초당 1만 줄 처리한다면 파티션이 최소 6개, 여유를 두어 12개쯤이 적당하다. 파티션은 늘릴 수는 있어도 줄일 수 없고, 늘리면 키 → 파티션 매핑이 바뀌므로 처음부터 넉넉히 잡는다. 너무 많으면(수천) 브로커 메타데이터와 파일 핸들 부담이 커진다. 이 프로젝트는 학습용으로 3개를 쓰고, Step 3에서 파티션을 늘려 보며 매핑이 바뀌는 것을 직접 본다.

## 확인 질문

1. 컨슈머 그룹 `log-printer`의 앱 3개와 `concurrency=3`인 앱 1개는 브로커 입장에서 무엇이 같고 무엇이 다른가?
2. `auto-offset-reset=latest`로 바꾸고 앱을 재시작하면 지난 메시지를 건너뛰는가? (답: 아니다. 왜?)
3. lag이 0인데 `consumed` 카운트가 Kafka UI의 메시지 수보다 작다. 가능한 이유는? (힌트: 앱 재시작, 카운터는 메모리)
4. 처리 도중 앱이 죽어 같은 메시지를 다시 받았다. `LogEventConsumer`는 이것을 알 수 있는가? Step 4의 적재기는 어떻게 알아야 하는가?
