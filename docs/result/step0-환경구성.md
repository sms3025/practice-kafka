# Step 0. 환경 구성 결과

## 만든 것

`docker/docker-compose.yml` 하나로 단일 브로커 Kafka(KRaft, combined 모드)와 Kafka UI를 띄운다. 브로커는 `apache/kafka:4.2.2` 공식 이미지를 쓰며, 설정은 전부 환경 변수(`KAFKA_` 접두어, 점을 밑줄로 바꾼 이름)로 넘긴다. 토픽 자동 생성은 꺼두었다(`auto.create.topics.enable=false`). 오타 난 토픽 이름으로 메시지를 보냈을 때 조용히 새 토픽이 생기는 사고를 막고, 토픽은 항상 코드나 CLI로 명시적으로 만든다는 습관을 들이기 위해서다.

포트 정리는 다음과 같다.

| 서비스 | 호스트 포트 | 용도 |
|---|---|---|
| kafka | 9092 | Spring Boot 앱 접속 (PLAINTEXT_HOST 리스너) |
| kafka (내부) | 19092 | 컨테이너 네트워크 내부 접속 (Kafka UI → 브로커) |
| kafka (내부) | 29093 | KRaft 컨트롤러 쿼럼 |
| kafka-ui | 8085 | http://localhost:8085 |

Kafka UI를 8080이 아니라 8085에 둔 이유는 Spring Boot 앱이 8080을 쓰기 때문이다.

## 실행 및 확인 절차

```bash
# 1. 인프라 기동
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps        # kafka 가 healthy 인지 확인

# 2. CLI로 토픽 만들기 (컨테이너 안의 스크립트 사용)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic hello --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello

# 3. 터미널 A: 컨슈머 (처음부터 읽기)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic hello --from-beginning --property print.key=true --property print.partition=true

# 4. 터미널 B: 프로듀서 (key:value 형식으로 입력)
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic hello --property parse.key=true --property key.separator=:
# > order:first
# > order:second
# > payment:third
```

터미널 A에서 `order` 키의 두 메시지가 같은 파티션 번호로 찍히고, `payment`는 다른(또는 우연히 같은) 파티션에 가는 것을 본다. 같은 키는 같은 파티션이라는 규칙을 눈으로 확인하는 것이 이 단계의 목적이다.

브라우저에서 http://localhost:8085 를 열면 Brokers 1개, Topics에 `hello`와 내부 토픽 `__consumer_offsets`가 보인다. Topics → hello → Messages 탭에서 방금 보낸 메시지와 파티션/오프셋을 확인한다.

## 정리

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic hello
docker compose -f docker/docker-compose.yml down      # -v 를 붙이면 볼륨(데이터)까지 삭제
```

## 배운 점 / 메모

- 리스너를 3개(PLAINTEXT, PLAINTEXT_HOST, CONTROLLER)로 나눠야 하는 이유는 `docs/concepts/01-kafka-basics.md`의 리스너 절에 정리했다. 호스트에서 `localhost:9092`로 붙었는데 전송이 타임아웃 나면 십중팔구 `ADVERTISED_LISTENERS` 문제다.
- `CLUSTER_ID`는 22자 base64 UUID 형식이어야 한다. 아무 문자열이나 넣으면 브로커가 기동하지 않는다.
- 단일 브로커에서는 `__consumer_offsets` 등 내부 토픽의 복제 계수를 1로 내려줘야 한다. 기본값 3이면 "브로커가 부족하다"는 오류로 컨슈머 그룹이 동작하지 않는다.
