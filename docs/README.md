# practice-kafka 아키텍처와 학습 가이드

이 문서는 저장소를 처음 여는 사람이 가장 먼저 읽는 한 장이다. 지금 무엇이 만들어져 있고(아키텍처), 코드와 문서가 어디에 있으며(지도), 어떤 순서로 읽고 돌려 보면 되는지(학습 경로)를 한 번에 설명한다. 세부 내용은 각 절에서 가리키는 문서로 내려간다. 앞으로 학습자료를 어떻게 만들어 갈지는 `plan/01-학습자료-제작-방침.md`에 있다.

## 1. 한 문단 요약

기업 연계 프로젝트 Log Collector는 반도체 공장의 OHT 차량 1,000대 이상이 각자 로컬 디스크에 쌓는 로그를 중앙으로 모아 저장하고 검색·시각화하는 시스템이다. 이 저장소는 그 구조를 축소판으로 직접 만들면서 Kafka를 배운다. 차량 대신 로그 생성기, 1,000대 대신 몇 대, 운영 서버 대신 docker 컨테이너를 쓰지만 구성요소와 데이터 흐름은 같다. 산출물은 동작하는 코드와, 그 코드를 "왜 이렇게 짰는지" 설명하는 문서다.

```
장비 로그 파일 ─ Fluent Bit ─ HTTP ─▶ Spring 수집 서버 ─▶ Kafka ─▶ 컨슈머 ─▶ ClickHouse ─▶ Grafana
   (생성기)      (docker)             (우리 코드)        (docker)   (우리 코드)   (Step 4)      (Step 4)
```

Kafka는 수집 서버와 저장소 사이의 완충 계층이다. 수집 서버는 Kafka에 쓰고 즉시 응답하고, 저장소 적재는 컨슈머가 자기 속도로 따라간다. 생산자와 소비자의 속도를 분리하는 것이 Kafka를 쓰는 첫째 이유이고, 같은 로그를 여러 소비자가 독립적으로 읽을 수 있다는 것이 둘째, 어느 구성요소가 죽어도 로그가 Kafka에 남아 복구 후 이어서 처리할 수 있다는 것이 셋째 이유다.

## 2. 아키텍처

이 절을 그림으로 보려면 `architecture.html`을 브라우저로 연다. 실행 구성도, 여섯 구간의 데이터 흐름(구간마다 들어오는 것과 나가는 것의 예), 스키마, 설계 결정, 단계 현황을 한 화면에 담았다. 단계가 끝날 때마다 이 절과 함께 갱신한다.

### 2.1 실행 시점의 구성

프로세스는 넷이다. 셋은 docker 컨테이너이고 하나는 IntelliJ나 Gradle로 호스트에서 띄우는 Spring Boot 앱이다. 호스트 폴더 `docker/devices`가 "차량의 로컬 디스크" 역할을 하며, 생성기가 여기에 쓰고 Fluent Bit 컨테이너가 여기서 읽는다.

```
 호스트 PC                                         docker compose (docker/docker-compose.yml)
 ─────────────────────────────────────            ──────────────────────────────────────────────
 로그 생성기                                       fluent-bit            :2020  지표
   scripts/ohtsim.cmd  (대외비 생성기, 실제 형식)       tail  /var/log/devices/**   ← docker/devices 마운트
   POST /api/logs/simulate (연습용 단순 형식)           http  host.docker.internal:8080/ingest/fluentbit
        │ 파일 쓰기                                        │
        ▼                                                   │ HTTP POST (JSON 배열)
 docker/devices/<장비ID>/...  ─────────────────────────────┘
                                                            ▼
 Spring Boot 앱  :8080                             kafka                 :9092  호스트용 리스너
   ingest   POST /ingest/fluentbit ──▶ producer ──────────▶   app-logs 토픽 (3 파티션, key=device_id)
   consumer @KafkaListener(log-printer) ◀────────────────────────┘      :19092 컨테이너 내부용 리스너
   api      /api/logs/* (실험용)                   kafka-ui              :8085  토픽·메시지·컨슈머 그룹·lag
```

| 구성요소 | 어디서 뜨는가 | 역할 | 설정 파일 |
|---|---|---|---|
| 로그 생성기 | 호스트 (Python) 또는 앱 내부 | 장비 로그 파일을 만든다. 실제 형식은 대외비 생성기, 연습용은 `DeviceLogSimulator` | `scripts/ohtsim.*`, `app.simulator.*` |
| Fluent Bit | docker | 파일을 tail해 HTTP로 수집 서버에 보낸다. 디스크 버퍼와 무한 재시도로 at-least-once | `docker/fluent-bit/fluent-bit.conf` |
| Spring 수집 서버 + 컨슈머 | 호스트 (JDK 25, Gradle이 자동 설치) | 받은 묶음을 정규화해 Kafka에 발행하고, 같은 프로세스의 리스너가 소비해 콘솔에 찍는다 | `src/main/resources/application.properties` |
| Kafka | docker (KRaft 단일 브로커) | `app-logs` 토픽에 메시지를 순서대로 쌓는다 | `docker/docker-compose.yml` |
| Kafka UI | docker | 브라우저에서 토픽, 메시지, 컨슈머 그룹, lag을 본다 | `docker/docker-compose.yml` |

수집 서버와 컨슈머가 한 프로세스에 있는 것은 학습 편의를 위한 것이다. 실제 프로젝트에서는 둘을 별도 서비스로 나눈다. Step 3에서 앱을 두 개 띄워 분리의 효과를 본다.

### 2.2 데이터 흐름 여섯 구간

로그 한 줄은 여섯 구간을 지난다. 각 구간에서 무엇이 바뀌는지가 이 프로젝트의 핵심 내용이며, `study/02-메시지의-여행.md`가 코드 한 줄씩 따라간다.

| 구간 | 들어오는 것 | 나가는 것 | 담당 |
|---|---|---|---|
| 1. 파일 → Fluent Bit | `.log`/`.txt` 파일의 새 줄 | 레코드 (`log`, `source`=파일 경로, `offset`=파일 내 바이트 위치) | `fluent-bit.conf` `[INPUT] tail` |
| 2. Fluent Bit → HTTP | 레코드 묶음 | JSON 배열 POST, `collected_at` 추가. 2xx가 아니면 재시도 | `fluent-bit.conf` `[OUTPUT] http` |
| 3. HTTP → 수집 서버 | JSON 배열 | `FluentBitRecord` 리스트 | `ingest/FluentBitIngestController` |
| 4. 정규화 | 원문 한 줄 + 경로 | `LogEvent` (device_id 추출, event_time UTC 변환, level/category/message 파싱) | `log/LogNormalizer` |
| 5. 발행 | `LogEvent` | `app-logs` 토픽, 키=device_id, 값=snake_case JSON. 브로커 ack 후 200 응답 | `producer/LogEventProducer` |
| 6. 소비 | 파티션의 메시지 | 콘솔 로그 (Step 4부터 ClickHouse 적재) | `consumer/LogEventConsumer` |

### 2.3 메시지 스키마

Kafka를 오가는 메시지는 명세서의 공통 스키마 여섯 필드에 파싱 필드 셋을 더한 것이다. 정의는 `src/main/java/com/practice/kafka/log/LogEvent.java` 하나에 있고, 바꾸면 `LogNormalizerTest`와 `study/02`를 함께 고친다.

| 필드 | 뜻 | 누가 채우는가 |
|---|---|---|
| `device_id` | 장비 식별자. Kafka 메시지 키 | 수집 서버가 파일 경로에서 추출 |
| `source` | 원본 파일 경로 | Fluent Bit (`Path_Key`) |
| `event_time` | 원문의 이벤트 시각을 UTC로 정규화 | 수집 서버. 못 찾으면 `collected_at`으로 대체 |
| `collected_at` | 에이전트 수집 시각 | Fluent Bit (`Json_date_key`) |
| `offset` | 파일 내 바이트 위치 | Fluent Bit (`Offset_Key`) |
| `raw` | 원문 원형 | Fluent Bit이 읽은 그대로 |
| `category`, `level`, `message` | 본문 파싱 결과 | 수집 서버. 실패하면 null |

멱등키는 `(device_id, source, offset)`이다. 에이전트가 같은 묶음을 다시 보내도 같은 줄은 같은 키를 가지므로, 저장소에서 중복을 걸러낼 수 있다. Kafka의 파티션 오프셋과 이름만 같고 전혀 다른 것이다.

### 2.4 코드 구조

패키지는 "바뀌는 이유"가 같은 것끼리 묶었다. 어떤 변경이 들어오면 어느 패키지를 열어야 하는지가 바로 보이게 하기 위해서다.

| 패키지 | 책임 | 바뀌는 때 |
|---|---|---|
| `ingest/` | `POST /ingest/fluentbit` 수신. 브로커 ack 후 2xx (at-least-once 계약) | 수집 에이전트가 바뀔 때 |
| `log/` | `LogEvent` 스키마, `LogNormalizer` 파서 | 로그 형식이나 스키마가 바뀔 때 |
| `producer/` | `KafkaTemplate` 발행, 부하 생성기 | Kafka 발행 정책이 바뀔 때 |
| `consumer/` | `@KafkaListener` | 적재 대상이 바뀔 때 |
| `config/` | 토픽 선언 | 토픽 구성이 바뀔 때 |
| `simulator/`, `api/` | 연습·실험용 | 실험 항목이 늘 때 |

설정은 `application.properties` 하나에 세 묶음으로 둔다. `spring.kafka.*`는 Spring Boot가 읽어 Kafka 클라이언트를 만들고, `app.*`는 이 프로젝트가 정의한 키이며, `logging.*`은 로그 레벨이다. 항목마다 주석으로 "왜"를 적어 두었으므로 파일을 직접 여는 것이 가장 빠르다.

### 2.5 지금 어디까지 되어 있는가

Step 2까지 구현됐다. 파일에서 Kafka까지의 수집 경로와 실제 OHT 로그 형식(대외비 생성기 출력) 파서가 코드와 문서로 있다. Step 1·2의 Spring 코드는 컴파일 검증 없이 작성됐다가 2026-10-13에 `./gradlew clean test`로 컴파일과 테스트 통과를 확인했다. 2026-10-14~15에 이미 떠 있던 docker 환경에서 B1~B3, D1, D3과 Kafka 정지 실험(C8, D5)을 실행해 확인했다. 그 과정에서 Windows 전용 버그 하나와 장애 시 예외 처리 누락 하나, 키 없는 메시지를 컨슈머가 버리는 문제 하나를 고쳤다(`practice/README.md` 7절). 깨끗한 환경에서 처음부터 재현하지는 않았으므로 상태는 여전히 "구현 완료, 실행 검증 전"이며, `practice/00`~`04`를 깨끗한 환경에서 따라 해 기대 결과가 나오면 "완료"가 된다. 그 뒤에도 없는 것은 셋이다. 컨슈머는 콘솔에 찍기만 하고 저장소가 없다(Step 4). 컨슈머가 하나뿐이라 파티션을 나눠 읽는 효과를 못 본다(Step 3). 브로커가 하나라 복제와 장애 복구를 못 본다(Step 6). 전체 단계 계획은 `plan/00-학습계획서.md`, 진행 표는 루트 `README.md`에 있다.

## 3. 문서 지도

문서는 다섯 폴더로 나뉘며, 폴더마다 답하는 질문이 다르다.

| 폴더 | 답하는 질문 | 언제 읽는가 |
|---|---|---|
| `plan/` | 무엇을 왜 만들기로 했는가. 단계 계획, 기술 선택 근거, 결정 이력, 자료 제작 방침 | 방향을 알고 싶을 때, 단계를 시작하기 전 |
| `study/` | 처음 보는 사람이 코드를 어떻게 읽고 돌려 보는가. 설명 → 코드 → 실습 → 확인 질문 | 학습의 본 교재. 순서대로 |
| `practice/` | 워크북의 실습 항목을 어느 터미널에서 어떤 명령으로 하고, 무엇이 보이면 넘어가는가 | 교재를 읽고 손으로 돌려 볼 때. `practice/README.md`부터 |
| `concepts/` | 이 용어는 무엇인가. Kafka 기본, Spring Kafka 계층, 주변 도구 | 교재를 읽다 모르는 말이 나올 때 |
| `result/` | 각 단계에서 무엇을 만들었고 어떻게 확인했으며 무엇을 배웠는가 | 단계를 끝낸 뒤 복기할 때, 실행 절차를 그대로 따라 할 때 |

질문별로 바로 가는 곳은 다음과 같다.

- 설치와 실행, 자주 나는 오류: `study/01-환경과-프로젝트-구조.md`
- 토픽·파티션·오프셋·컨슈머 그룹이 무엇인지: `concepts/01-kafka-basics.md`. 그림과 가상 로그를 따라가려면 [Kafka 기초 HTML](concepts/01-kafka-basics.html)을 브라우저로 연다.
- `KAFKA_LISTENERS`와 `KAFKA_ADVERTISED_LISTENERS`가 왜 둘인지, 접속이 어디서 끊기는지: [리스너 HTML](concepts/01-kafka-listeners.html)에서 클라이언트와 설정을 바꿔 가며 접속 단계를 따라간다.
- 로그 한 줄이 코드의 어디를 지나는지: `study/02-메시지의-여행.md`
- `application.properties`의 producer 항목 하나하나: `study/03-Producer-깊게.md`
- consumer 항목, 오프셋 커밋, 리밸런스: `study/04-Consumer-깊게.md`
- `KafkaTemplate`과 `@KafkaListener` 뒤의 Spring 계층: `concepts/02-spring-kafka.md`
- Fluent Bit, Kafka UI, ClickHouse, Grafana가 각각 무엇인지: `concepts/03-주변-도구.md`
- 대외비 생성기로 실제 형식 로그 흘리기: `study/05-OHT-로그-연동.md`
- 체크리스트식 실습과 기대 결과: `study/06-실습-워크북.md`. 같은 실습을 명령 하나씩 따라 하려면 `practice/README.md`
- 각 단계의 실행 절차와 배운 점: `result/step0`, `step1`, `step2`

## 4. 어디서부터 어떻게 공부하는가

### 4.1 사전 지식

Java와 Spring Boot의 기본(`@Component`, 의존성 주입, `application.properties`)은 안다고 가정한다. Docker는 `docker compose up -d`, `docker ps`, `docker logs` 세 명령이면 된다. Kafka는 몰라도 된다.

### 4.2 학습의 기본 단위

이 저장소의 학습은 "읽는다 → 돌린다 → 바꿔 본다 → 말로 설명한다"의 반복이다. 문서만 읽고 넘어가면 남는 것이 적다. 각 장 끝의 확인 질문에 스스로 답하고, 워크북의 체크박스를 채우며 측정값을 빈칸에 적는다. 설정값을 바꾸고 재시작해 Kafka UI와 콘솔이 어떻게 달라지는지 보는 것이 가장 효과가 크다.

### 4.3 추천 경로

처음 보는 사람 기준으로 세 묶음, 총 10시간 안팎이다. 각 묶음은 "읽기 → 실행 → 실습 → 질문"으로 끝낸다.

**묶음 1. 환경을 띄우고 큰 그림을 잡는다 (2시간)**

1. 이 문서 1절과 2절을 읽는다.
2. `study/01`로 설치하고 띄운다. 먼저 `./gradlew test`로 테스트가 통과하는지 보고(docker 없이 돈다), 앱을 띄워 기동 로그에서 `partitions assigned`를 확인한다. PATH에 Java가 없으면 IntelliJ 내장 JDK를 `JAVA_HOME`으로 잡는다.
3. `concepts/01`로 용어를 익힌다.
4. `practice/00`~`02`를 따라 워크북 A절(Step 0)과 B절(Step 1)을 한다. CLI로 메시지를 주고받고, REST로 한 건 발행해 콘솔에서 소비를 본다.
5. 질문: 토픽, 파티션, 오프셋, 컨슈머 그룹의 관계를 말로 설명한다.

**묶음 2. 로그 한 줄의 여행을 코드로 따라간다 (4시간)**

1. `study/02`를 코드를 열어 두고 읽는다. 구간마다 담당 파일로 가서 해당 줄을 찾는다.
2. `study/03`을 읽고 `practice/03`을 따라 워크북 C절(Producer 설정 실험)을 한다. 측정값을 표에 적는다.
3. `study/04`를 읽는다. 실험은 Step 3에서 하므로 지금은 개념만 잡는다.
4. `practice/04`를 따라 워크북 D절(Fluent Bit 수집 경로)을 한다. D4와 D5의 "수집 서버 다운", "Kafka 다운" 실험으로 유실 0을 직접 확인하고, C8에서 같은 장애로 메시지가 사라진 것과 비교한다.
5. 질문: at-least-once가 무엇이며 이 파이프라인의 어느 두 구간에서 중복이 생길 수 있는지, 무엇으로 걸러낼 것인지 설명한다.

**묶음 3. 실제 로그 형식과 도구들 (2~3시간)**

1. `concepts/03`으로 주변 도구의 역할을 익힌다.
2. 대외비 생성기가 있으면 `study/05`를 읽고 `practice/05`를 따라 워크북 E절을 한다. 없으면 `study/05`의 1~2절만 읽고 형식 설명을 이해한다.
3. `result/step2`의 "설계에서 눈여겨볼 것"과 "결정 비교"를 읽는다. 왜 Fluent Bit에서 Kafka로 직접 쓰지 않는지가 핵심이다.
4. 워크북 F절의 다섯 가지를 각각 다섯 문장 이내로 말할 수 있으면 Step 3으로 갈 준비가 된 것이다.

Spring Kafka는 알고 이 프로젝트의 구조만 빨리 파악하려는 사람은 이 문서 2절, `study/02`, `result/step2` 세 개만 읽으면 된다. 1시간이면 충분하다.

### 4.4 단계별로 무엇이 덧붙는가

앞으로의 단계는 각각 구성요소나 설정을 하나씩 더한다. 단계가 끝날 때마다 `result/stepN`과 `study`의 새 장이 생기고 워크북에 절이 추가된다. 무엇이 언제 만들어지는지는 `plan/01-학습자료-제작-방침.md`의 산출물 표에 있다.

| 단계 | 덧붙는 것 | 배우는 것 |
|---|---|---|
| Step 3 | `concurrency` 증가, 앱 2개 기동, 두 번째 컨슈머 그룹 | 파티션과 컨슈머의 관계, 리밸런스, 그룹 간 브로드캐스트, 파티션 수 산정 |
| Step 4 | ClickHouse, Grafana 컨테이너, 배치 리스너, `clickhouse-sink` 그룹. ClickHouse 단독 체험부터 네 묶음으로 나눠 진행 | part와 병합, 정렬 키와 파티션 키, 배치 소비, 멱등키로 중복 제거(ReplacingMergeTree), TTL, 외부 저장소 실패 시 커밋 전략 |
| Step 4+ | `compare` 프로파일, `/ingest/direct`, 경로별 Fluent Bit과 테이블, Kafka 테이블 엔진 | 같은 입력으로 직접 적재와 Kafka 경유를 비교. part 수, 지연, ClickHouse 장애 시 복구, 재처리 가능 여부 |
| Step 5 | `DefaultErrorHandler`, Dead Letter Topic, 멱등 프로듀서, gzip 전송 | 재시도와 DLT, at-most/at-least/exactly-once, 전송 최적화 |
| Step 6 | 3브로커 compose, `replicas=3`, `min.insync.replicas=2` | 복제, 리더 선출, 브로커 장애 실험 |
| Step 7 (선택) | Kafka Streams 집계 | 장비별 1분 ERROR 집계, 임계치 알림 토픽 |

### 4.5 막혔을 때

오류가 나면 `study/01` 7절의 표를 먼저 본다. 리스너 주소, 토픽 없음, trusted packages, 오프셋 때문에 안 읽히는 경우 등 입문자가 겪는 대부분이 거기 있다. 표에 없는 오류를 해결했으면 그 표에 한 줄 추가한다. 이것이 학습자료가 자라는 방식이다.

Kafka UI(http://localhost:8085)와 Fluent Bit 지표(http://localhost:2020/api/v1/metrics), 앱의 `/api/logs/stats`를 함께 보면 파이프라인의 어느 구간이 막혔는지 알 수 있다. Fluent Bit의 input 건수와 output 건수가 다르면 전송 문제, `receivedFromFluentBit`와 `consumed`가 다르면 Kafka 또는 컨슈머 문제, Kafka UI의 lag이 늘기만 하면 컨슈머가 느린 것이다.

## 5. 대외비 규칙

기업이 제공한 로그 생성기 `ohtsim.pyz`, 그 사용법 문서, 생성된 로그는 대외비이며 이 저장소는 GitHub에 공개되어 있다. `.gitignore`가 `tools/`, `docker/devices/*`, `*.pyz`, `사용법.md`를 막고 있으며 절대 `git add -f` 하지 않는다. 문서, 테스트, 커밋 메시지, 주석에 생성 로그의 실제 내용을 붙여 넣지 않고, 형식 설명이 필요하면 형식만 같은 가짜 문장을 쓴다. 커밋 전 `git status`로 tracked 파일을 확인한다. 자세한 것은 `study/05` 0절에 있다.
