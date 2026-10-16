# 03. Producer 깊게: KafkaTemplate 안에서 벌어지는 일과 설정의 의미

## 1. 코드는 한 줄, 내부는 다섯 단계

```java
kafkaTemplate.send(topic, event.deviceId(), event)
```

`KafkaTemplate`은 Apache `KafkaProducer`를 감싼 Spring 클래스다. Spring Boot가 `spring.kafka.producer.*`를 읽어 `ProducerFactory`를 만들고, 거기서 `KafkaProducer` 인스턴스 하나를 만들어 `KafkaTemplate`에 넣는다. **프로듀서는 앱 전체에서 하나**이며 스레드 안전하다. REST 요청마다 새로 만들지 않는다(만들면 TCP 연결과 메타데이터 조회가 매번 일어나 매우 느리다).

`send()`를 부르면 다음이 순서대로 일어난다.

```
send(topic, key, value)
  │
  ├─ 1. 인터셉터 (없음)
  ├─ 2. 직렬화          key → byte[]  (StringSerializer)
  │                     value → byte[] (JacksonJsonSerializer → JSON)
  ├─ 3. 파티셔너         partition = murmur2(keyBytes) % numPartitions   (키가 있을 때)
  ├─ 4. RecordAccumulator 에 추가   파티션별 배치 큐에 레코드를 붙인다   ← send() 는 여기서 반환
  │
  └─ (별도 Sender 스레드)
       5. linger.ms 경과 또는 batch.size 도달 → 배치를 브로커(파티션 리더)로 전송
       6. acks 조건 만족한 응답 수신 → CompletableFuture 완료 (또는 재시도/실패)
```

이 구조에서 두 가지를 기억하면 된다. `send()`는 **비동기**라 반환 시점에 전송이 끝나지 않았다는 것, 그리고 레코드는 **파티션별로 묶여(batch)** 나간다는 것이다.

> 심화(지금은 넘어가도 된다): 프로듀서 내부의 세 가지 의문.
>
> **프로듀서 하나를 모든 스레드가 같이 쓰면 느리지 않은가.** 느리지 않고 대개 더 빠르다. 요청 스레드가 `send()`에서 하는 일은 직렬화, 파티션 결정, 버퍼에 덧붙이기뿐이라 마이크로초 단위로 끝나고, 잠금도 파티션별 큐에만 짧게 걸린다. 네트워크 I/O는 Sender 스레드 하나가 전담한다. 여러 스레드의 메시지가 같은 배치에 모이므로 배치가 빨리 차고 요청 수가 줄어든다. 프로듀서를 여러 개 두면 배치가 잘게 쪼개지고 연결과 버퍼 메모리만 늘어난다. 프로듀서를 나누는 것은 Sender 스레드 하나가 병목이 될 만큼 처리량이 클 때, 또는 트랜잭션을 쓸 때(트랜잭션은 프로듀서 단위다)다. `send()`가 요청 스레드를 붙잡는 경우는 둘이다. 버퍼(`buffer.memory`, 기본 32MB)가 가득 찼을 때, 그리고 보낼 토픽의 메타데이터(파티션 수와 리더)를 모를 때다. 둘 다 최대 `max.block.ms`(기본 60초)까지 기다리고, 넘으면 `Future`가 아니라 예외로 바로 실패한다. 둘 다 브로커가 받아 주지 못한다는 신호이지 프로듀서 개수의 문제가 아니다. 두 번째 경우는 이 프로젝트에서 실제로 겪는다. 단일 브로커를 멈추면 Kafka 4 클라이언트는 메타데이터를 버리고 bootstrap부터 다시 시작한다(`metadata.recovery.strategy=rebootstrap`). 그래서 `send()`가 60초 동안 막힌다(`practice/03` C8).
>
> **RecordAccumulator는 무엇인가.** 프로듀서 안의 메모리 버퍼로, 구조는 `Map<TopicPartition, Deque<ProducerBatch>>`다. `send()`는 해당 파티션 큐의 마지막 배치에 레코드를 붙이고(차 있으면 새 배치를 만든다) 빈 `Future`를 돌려준다. Sender 스레드는 준비된 배치(`batch.size`가 찼거나, `linger.ms`가 지났거나, `flush()`가 불렸거나)를 꺼내 **리더 브로커별로 묶어** 요청 하나에 여러 파티션의 배치를 실어 보낸다. 압축도 배치 단위로 이 시점에 일어난다. 재시도할 배치는 큐 앞쪽으로 되돌아간다.
>
> **네트워크로 나가는 것은 언제인가.** Sender가 배치를 꺼내 소켓에 쓸 때다. `send()`가 반환된 시점의 메시지는 아직 앱의 JVM 메모리에만 있다. 다만 "소켓에 썼다"도 "기록됐다"는 아니다. 기록이 보장되는 것은 `acks` 조건을 만족한 응답이 와서 `Future`가 완료될 때다. 그래서 `FluentBitIngestController`는 `join()`으로 이 완료를 기다린 뒤에야 2xx를 준다. 그 전에 앱이 죽으면 버퍼 안의 메시지는 사라지지만, Fluent Bit이 2xx를 못 받았으므로 다시 보낸다. 하나 더, `whenComplete` 콜백은 Sender 스레드에서 실행된다. 콜백에서 무거운 일을 하면 그동안 모든 전송이 멈추므로 로그 정도만 남긴다.
>
> 더 파고들 곳은 Apache Kafka 소스의 `KafkaProducer`, `RecordAccumulator`, `Sender` 세 클래스다. Step 5에서 압축과 멱등 프로듀서를 다룰 때 배치 구조를 다시 만난다.

## 2. 왜 비동기인가, 그래서 어떻게 써야 하는가

동기로 한 건씩 보내면 매 건마다 네트워크 왕복을 기다려야 하므로 초당 수백 건이 한계다. 비동기 + 배치면 같은 하드웨어로 초당 수십만 건이 나온다. 대신 "보냈다"와 "브로커에 기록됐다" 사이에 시간 차가 생기고, 그 사이에 실패할 수 있다.

`LogEventProducer.send()`가 `CompletableFuture`를 돌려주는 것은 호출자가 이 시간 차를 어떻게 다룰지 선택하게 하기 위해서다. 이 프로젝트에는 세 가지 쓰임이 있다.

```java
// (1) 결과를 기다린다. 에이전트에게 정확한 성공/실패를 알려야 하는 수집 서버
CompletableFuture.allOf(futures).join();        // FluentBitIngestController

// (2) 결과를 기다리지 않는다. 빠른 응답이 중요한 실험용 REST
producer.send(event);  return 202;              // LogController.publish

// (3) 전부 보내고 한꺼번에 기다려 처리량을 잰다
for (...) futures[i] = producer.send(...);  allOf(futures).join();   // LogGenerator
```

(3)이 흥미롭다. 5,000건을 `send()`하는 데는 수 ms밖에 안 걸린다(버퍼에 넣기만 하니까). 그 뒤 `join()`에서 실제 전송과 ack를 기다린다. `POST /api/logs/generate?count=5000`이 돌려주는 `elapsedMs`가 그 시간이다. 이 숫자로 설정 변경의 효과를 잰다.

## 3. 설정 하나하나

`application.properties`의 producer 항목이다. 각각을 바꿔보며 `generate`의 `elapsedMs`와 Kafka UI를 관찰한다.

### 직렬화기

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
spring.kafka.producer.properties[spring.json.add.type.headers]=false
```

Kafka는 바이트만 나르므로 객체를 바이트로 바꾸는 규칙을 양쪽이 합의해야 한다. 이 프로젝트는 JSON을 쓴다. 사람이 읽을 수 있고(Kafka UI에서 바로 보인다), 언어에 독립적이다. 운영 규모에서는 스키마가 강제되고 크기가 작은 Avro나 Protobuf + Schema Registry를 많이 쓰지만, 학습 단계에서는 JSON이 적합하다.

`add.type.headers=false`는 Spring이 메시지 헤더에 Java 클래스 이름(`__TypeId__`)을 붙이지 않게 한다. 붙이면 컨슈머가 그 이름으로 역직렬화하므로 편하지만, 프로듀서와 컨슈머의 패키지가 달라지면 깨지고 Java가 아닌 소비자에게는 쓰레기 헤더다. 대신 컨슈머 쪽에서 `default.type`으로 클래스를 지정한다.

### acks: 무엇을 "성공"으로 볼 것인가

```properties
spring.kafka.producer.acks=all
```

| 값 | 브로커가 응답하는 시점 | 유실 가능성 | 속도 |
|---|---|---|---|
| `0` | 응답하지 않음. 보내자마자 성공 처리 | 네트워크 오류도 모름 | 가장 빠름 |
| `1` | 리더 파티션이 자기 로그에 쓰면 | 리더가 복제 전에 죽으면 유실 | 중간 |
| `all` (`-1`) | ISR(동기화된 복제본 전부)이 쓰면 | `min.insync.replicas` 이상이면 안전 | 가장 느림 |

단일 브로커에서는 복제본이 리더 하나뿐이라 `1`과 `all`이 같다. 차이는 Step 6(3브로커)에서 드러난다. 그래도 지금부터 `all`로 두는 이유는, 로그 수집에서 "받았다고 했는데 유실"은 "느리다"보다 훨씬 나쁜 결과이기 때문이다. Kafka 3.0부터 기본값도 `all`이다.

### 배치: linger.ms 와 batch.size

```properties
spring.kafka.producer.batch-size=16384
spring.kafka.producer.properties[linger.ms]=5
```

Sender 스레드는 파티션별 배치가 `batch.size`(16KB)에 차거나, 첫 레코드가 들어온 뒤 `linger.ms`(5ms)가 지나면 그 배치를 보낸다. `linger.ms=0`이면 레코드가 들어오는 즉시 보내므로 배치가 거의 안 묶이고 요청 수가 폭증한다. `linger.ms=50`이면 50ms 동안 모아 보내므로 요청당 레코드가 많아져 처리량은 오르지만 각 메시지의 지연이 최대 50ms 늘어난다. 처리량과 지연의 전형적인 트레이드오프다.

실험: `linger.ms`를 0, 5, 50으로 바꿔 `generate?count=20000`의 `elapsedMs`를 비교한다. 단일 브로커 로컬이라 차이가 극적이진 않지만 방향은 보인다. 요청 수와 배치 크기는 앱의 Actuator로 본다. 프로듀서 내부 지표가 `/actuator/metrics/kafka.producer.request.total`, `kafka.producer.batch.size.avg`, `kafka.producer.records.per.request.avg` 같은 이름으로 나온다. 이 compose는 브로커 JMX를 열지 않았으므로 Kafka UI의 Brokers → Metrics에는 값이 없다. 측정을 한 번에 하는 스크립트는 `../practice/03-프로듀서-설정-실험.md` C0에 있다. 2026-10-15 기준 측정에서는 `generate`의 배치가 평균 15.7KB로 `batch.size`(16KB)에 거의 꽉 찼다. 몰아치는 부하에서는 `linger.ms`를 기다리기 전에 배치가 차므로, `linger.ms`보다 `batch.size`의 효과가 크게 보일 것이다.

실제 로그로 처리량 한계를 잰 결과(`../result/benchmark-kafka-throughput.md`)에서는 이 두 값이 천장을 정했다. 프로듀서는 브로커 연결 하나에 응답을 기다리지 않은 요청을 최대 5개까지만 보내므로, 처리량은 "5 × 요청당 레코드 수 ÷ 요청 한 번의 왕복 시간"을 넘지 못한다. 16KB 배치로는 초당 40만 줄(docker 안, 왕복 약 1ms)에서 막혔고, 호스트의 앱에서 보낼 때는 왕복이 약 5ms로 길어져 10만 줄도 못 버텼다. `batch.size=131072`, `linger.ms=20`으로 바꾸자 같은 브로커가 90만 줄을 p99 10ms로 받았다. 배치를 키웠는데 지연이 오히려 준 것은, 요청 수 상한에 닿으면 레코드가 버퍼에서 줄을 서기 때문이다.

### 재시도와 멱등성

```properties
spring.kafka.producer.retries=3
```

브로커가 일시적 오류(리더 선출 중, 네트워크 끊김)를 돌려주면 프로듀서가 자동으로 재시도한다. 실제 상한은 `retries`보다 `delivery.timeout.ms`(기본 2분)다. 그 안에 성공 못 하면 `CompletableFuture`가 예외로 완료된다. 이것은 레코드가 일단 버퍼에 들어간 뒤의 이야기다. 브로커가 전부 내려가 메타데이터부터 없으면 레코드는 버퍼에 들어가지도 못하고, `send()` 호출 자체가 `max.block.ms`(기본 60초) 뒤 예외를 던진다. 이 프로젝트처럼 브로커가 하나면 Kafka를 멈추는 순간 이 경우가 된다(2026-10-15 실측).

재시도에는 함정이 있다. 브로커가 기록은 했는데 응답이 유실되면 프로듀서는 재전송하고 **같은 메시지가 두 번 기록**된다. 이를 막는 것이 `enable.idempotence=true`(Kafka 3.0+ 기본값)다. 프로듀서가 각 배치에 일련번호를 붙이고 브로커가 중복을 걸러낸다. Spring Boot 4 + Kafka 4 조합에서는 기본으로 켜져 있으므로 이 프로젝트의 Kafka 구간은 이미 중복이 없다. 하지만 **Fluent Bit → 수집 서버 구간의 재전송은 Kafka 멱등성과 무관**하다. 그 중복을 걸러내는 것이 `(device_id, source, offset)` 멱등키다. Step 5에서 정리한다.

### 압축

```properties
# spring.kafka.producer.compression-type=lz4
```

지금은 꺼져 있다. 켜면 배치 단위로 압축해 보낸다. 로그처럼 반복이 많은 텍스트는 lz4로 3~5배, zstd로 그 이상 줄어든다. 네트워크와 브로커 디스크를 아끼는 대신 프로듀서 CPU를 쓴다. 명세서의 "전송 최적화(압축)"는 Fluent Bit → 수집 서버 구간(gzip)과 이 구간 둘 다 해당한다. Step 5에서 켜고 브로커 디스크 사용량(`docker exec kafka du -sh /var/lib/kafka/data`)을 비교한다. 부하 테스트에서 실제 OHT 로그 JSON은 lz4로 0.23배(4.3분의 1)가 됐다. 네트워크가 좁은 경로에서는 압축이 처리량을 올렸고(앱 경유 약 10만 → 13.9만 줄/s), 넓은 경로에서는 보내는 쪽 CPU만 더 써서 천장을 낮췄다(브로커 단독 90만 → 70만). 압축은 "켜면 좋다"가 아니라 대역과 CPU 중 무엇이 모자란지에 따라 정한다(`../result/benchmark-kafka-throughput.md` 7절).

## 4. 파티셔너와 키

키가 있으면 `murmur2(key) % 파티션수`, 없으면 **sticky** 방식(한 배치가 찰 때까지 같은 파티션에 붙이고 다음 배치는 다른 파티션)으로 분산한다.

키를 `deviceId`로 잡은 결과를 정리하면, 같은 장비의 로그는 한 파티션에 순서대로 들어간다(장비 단위 순서 보장). 1,000대 장비가 3개 파티션에 해시로 분산되므로 파티션 간 균형이 대체로 맞는다. 단 파티션 수를 나중에 늘리면 `% 파티션수` 결과가 바뀌어 같은 장비가 다른 파티션으로 가기 시작한다. 늘린 시점 전후로 순서 보장이 잠깐 깨진다. 그래서 파티션 수는 처음에 여유 있게 잡는 것이 관례다(Step 3에서 다룬다).

실험: `LogEventProducer.send()`에서 키를 `null`로 바꾸고 `generate?count=300&devices=3`을 보낸 뒤 Kafka UI Messages 탭에서 같은 `device_id`가 여러 파티션에 흩어지는 것을 본다. 다시 키를 복구한다.

## 5. 실패를 어떻게 다루는가

`whenComplete`의 `ex != null` 분기가 전송 실패다. 지금은 로그만 남긴다. 실제 프로젝트에서는 선택지가 세 가지다. 호출자에게 실패를 돌려줘 상위에서 재시도하게 하거나(수집 서버가 Fluent Bit에 500을 주는 것이 이것), 로컬 디스크에 적어두고 나중에 재발행하거나, 포기하고 메트릭만 올린다. 로그 수집은 첫 번째가 맞다. 에이전트가 이미 재시도 장치를 갖고 있기 때문이다.

실험: Kafka를 멈추고(`docker stop kafka`) `POST /api/logs/simulate?lines=50`으로 로그를 쓴 뒤 앱 콘솔을 본다. 60초 뒤 `Kafka 발행 실패. Fluent Bit 에 재전송을 요청한다` ERROR 로그와 함께 Fluent Bit에 500이 나가고, `docker logs fluent-bit`에 retry가 찍힌다. 60초인 이유는 위 "재시도와 멱등성"의 `max.block.ms`다. `docker start kafka` 후 밀린 것이 들어오는 것을 확인한다. 유실 건수는 0이어야 한다(`/api/logs/stats`의 `consumed`로 센다). 같은 장애에서 `POST /api/logs`로 보낸 한 건은 사라진다. 둘을 나란히 해 보는 절차는 `../practice/03-프로듀서-설정-실험.md` C8과 `../practice/04-fluent-bit-수집경로.md` D5에 있다.

이 실험에서 하나 더 드러난 것이 있다. 처음 코드는 `send()`가 `Future`로만 실패한다고 가정해 `join()`만 try로 감쌌다. 그래서 `send()` 자체가 던진 예외는 컨트롤러 밖으로 새어 나갔다. 500은 Spring의 기본 오류 처리가 대신 돌려줬고, 위의 ERROR 로그는 남지 않았다. 2026-10-15에 `send()` 루프까지 try 안으로 옮겼다(`FluentBitIngestController`). 비동기 API라도 호출 시점에 동기 예외를 던질 수 있다는 것을 기억한다.

## 확인 질문

1. `send()`가 반환됐을 때 메시지는 어디에 있는가? 그 직후 앱이 `kill -9`로 죽으면 메시지는 어떻게 되는가?
2. `acks=0`으로 바꾸면 `FluentBitIngestController`의 `join()`은 무엇을 기다리게 되는가? 수집 서버의 200 응답이 여전히 "Kafka에 기록됐다"를 의미하는가?
3. `linger.ms`를 1000으로 올리면 `generate?count=10`은 얼마나 걸리는가? 왜 그런가?
4. 파티션 수를 3에서 6으로 늘리면 `OHT-0007`의 로그는 계속 같은 파티션으로 가는가?
