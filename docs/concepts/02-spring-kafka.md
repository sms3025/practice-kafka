# 02. Spring for Apache Kafka 구조

## 계층 구조

```
내 코드 (LogEventProducer, LogEventConsumer)
   │
Spring for Apache Kafka  (KafkaTemplate, @KafkaListener, 리스너 컨테이너, 에러 핸들러)
   │
kafka-clients (Apache 공식 Java 클라이언트: KafkaProducer, KafkaConsumer, AdminClient)
   │
TCP ── 브로커
```

Spring Boot는 `spring-boot-starter-kafka`를 넣으면 `spring.kafka.*` 설정을 읽어 아래 빈들을 자동으로 만든다.

| 자동 구성 빈 | 역할 | 설정 접두어 |
|---|---|---|
| `ProducerFactory` / `KafkaTemplate` | 메시지 전송 | `spring.kafka.producer.*` |
| `ConsumerFactory` / `ConcurrentKafkaListenerContainerFactory` | `@KafkaListener` 메서드를 돌리는 컨테이너 생산 | `spring.kafka.consumer.*`, `spring.kafka.listener.*` |
| `KafkaAdmin` | `NewTopic` 빈을 보고 토픽 생성 | `spring.kafka.admin.*` |

우리가 직접 `@Bean`을 만들지 않아도 되는 이유가 이것이다. 설정을 properties로 바꾸는 것만으로 동작이 바뀌므로, 학습 단계에서 각 옵션의 효과를 실험하기 좋다.

## KafkaTemplate.send()의 실제 흐름

`kafkaTemplate.send(topic, key, value)`를 호출하면 다음이 일어난다.

1. 직렬화기(serializer)가 키와 값을 `byte[]`로 바꾼다. 우리는 `StringSerializer`와 `JacksonJsonSerializer`를 썼다.
2. 파티셔너가 키 해시로 파티션 번호를 정한다.
3. 레코드는 `RecordAccumulator`라는 메모리 버퍼에 파티션별로 쌓인다. **여기서 메서드는 바로 반환된다.**
4. 별도의 `Sender` I/O 스레드가 `linger.ms`가 지나거나 `batch.size`가 차면 배치를 묶어 브로커로 보낸다.
5. 브로커 응답(`acks` 기준 충족)이 오면 `CompletableFuture`가 완료된다.

그래서 `send()`의 반환값을 무시하면 전송 실패를 알 수 없다. `LogEventProducer`에서 `whenComplete`로 결과를 로깅하는 이유다. 응답을 반드시 확인해야 하는 경우(결제 등)는 `.get()`으로 동기 대기하지만, 로그처럼 처리량이 중요한 경우에는 비동기로 흘려보내고 실패만 별도 처리하는 편이 일반적이다.

## @KafkaListener의 실제 흐름

`@KafkaListener`가 붙은 메서드마다 Spring은 `KafkaMessageListenerContainer`를 만든다. 컨테이너는 자기 스레드에서 다음 루프를 영원히 돈다.

```
while (running) {
    records = consumer.poll(pollTimeout);     // 브로커에서 메시지 묶음을 당겨온다 (기본 최대 500건)
    for (record : records) {
        listenerMethod.invoke(record);         // 우리 메서드 호출
    }
    consumer.commitSync() 또는 commitAsync();  // ack 모드에 따라 오프셋 커밋
}
```

기본 ack 모드는 `BATCH`로, poll로 받은 묶음을 전부 처리한 뒤 커밋한다. 처리 도중 앱이 죽으면 커밋 전이므로 재시작 후 같은 묶음을 다시 받는다. 이것이 **at-least-once** 이며 Kafka의 기본 보장이다. 따라서 컨슈머 로직은 같은 메시지를 두 번 받아도 괜찮도록(멱등하게) 짜는 습관이 필요하다.

`concurrency = N`으로 두면 컨테이너가 N개의 내부 컨슈머(스레드)를 만들며, 이들은 모두 같은 그룹이므로 파티션을 나눠 갖는다. 파티션이 3개인데 concurrency를 5로 두면 2개 스레드는 놀게 된다.

## JSON 직렬화와 타입 정보

`JacksonJsonSerializer`는 기본적으로 메시지 헤더에 `__TypeId__`(Java 클래스 이름)를 넣어 컨슈머가 어떤 클래스로 복원할지 알게 한다. 편리하지만 두 가지 문제가 있다. 첫째, 프로듀서와 컨슈머의 패키지 경로가 다르면 깨진다. 둘째, Java가 아닌 컨슈머에게는 쓸모없는 헤더다. 그래서 이 프로젝트는 `spring.json.add.type.headers=false`로 헤더를 끄고, 컨슈머에서 `spring.json.value.default.type`으로 복원할 클래스를 직접 지정했다. 토픽 하나에 메시지 타입 하나를 두는 설계라면 이 방식이 깔끔하다.

`spring.json.trusted.packages`는 역직렬화를 허용할 패키지 목록이다. 임의의 클래스를 역직렬화하게 두면 보안 취약점이 되므로 Spring이 기본으로 막아둔다.

## Spring Boot 4 / Spring Kafka 4에서 달라진 점

이 프로젝트가 3.x 자료와 다른 부분을 정리한다.

- 스타터가 세분화되어 웹은 `spring-boot-starter-webmvc`를 쓴다(`spring-boot-starter-web`도 호환용으로 남아 있다).
- Jackson이 3.0으로 올라가면서 직렬화기 클래스가 `JsonSerializer` → `JacksonJsonSerializer`, `JsonDeserializer` → `JacksonJsonDeserializer`로 바뀌었다. 기존 이름은 Jackson 2용으로 deprecated 상태다.
- Kafka 클라이언트가 4.x이므로 브로커도 KRaft 기반 4.x를 쓴다. 테스트용 `@EmbeddedKafka`도 KRaft로 뜬다.
