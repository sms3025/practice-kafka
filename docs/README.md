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

프로세스는 여섯이다. 다섯은 docker 컨테이너이고 하나는 IntelliJ나 Gradle(또는 `java -jar`)로 호스트에서 띄우는 Spring Boot 앱이다. 호스트 폴더 `docker/devices`가 "차량의 로컬 디스크" 역할을 하며, 생성기가 여기에 쓰고 Fluent Bit 컨테이너가 여기서 읽는다.

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
   ingest   POST /ingest/fluentbit ──▶ producer ──────────▶   app-logs 토픽 (6 파티션, key=device_id)
   consumer log-printer, log-stats  ◀────────────────────────────┤      :19092 컨테이너 내부용 리스너
   sink     clickhouse-sink (배치)  ◀────────────────────────────┘
        │ RowBinary INSERT (localhost:8123)
        └────────────────────────────────────────▶ clickhouse            :8123  logs.app_logs (SQL 창 /play)
   api      /api/logs/*, /api/sink/stats           grafana               :3000  대시보드 (ClickHouse 를 읽음)
                                                   kafka-ui              :8085  토픽·메시지·컨슈머 그룹·lag
```

| 구성요소 | 어디서 뜨는가 | 역할 | 설정 파일 |
|---|---|---|---|
| 로그 생성기 | 호스트 (Python) 또는 앱 내부 | 장비 로그 파일을 만든다. 실제 형식은 대외비 생성기, 연습용은 `DeviceLogSimulator` | `scripts/ohtsim.*`, `app.simulator.*` |
| Fluent Bit | docker | 파일을 tail해 HTTP로 수집 서버에 보낸다. 디스크 버퍼와 무한 재시도로 at-least-once | `docker/fluent-bit/fluent-bit.conf` |
| Spring 수집 서버 + 컨슈머 | 호스트 (JDK 25, Gradle이 자동 설치) | 받은 묶음을 정규화해 Kafka에 발행한다. 같은 프로세스에서 세 그룹이 소비한다: 콘솔 출력(log-printer), 레벨별 건수(log-stats), ClickHouse 적재(clickhouse-sink) | `src/main/resources/application.properties` |
| Kafka | docker (KRaft 단일 브로커) | `app-logs` 토픽(파티션 6)에 메시지를 순서대로 쌓는다 | `docker/docker-compose.yml` |
| Kafka UI | docker | 브라우저에서 토픽, 메시지, 컨슈머 그룹, lag을 본다 | `docker/docker-compose.yml` |
| ClickHouse | docker (Step 4) | 로그를 열 지향으로 압축해 저장한다. 멱등키로 중복을 지우고 TTL로 보존 기간을 관리한다 | `docker/clickhouse/init/01-schema.sql` |
| Grafana | docker (Step 4) | ClickHouse를 읽어 대시보드를 그린다. 데이터 소스와 대시보드는 파일로 넣는다 | `docker/grafana/` |

Step 4+의 비교 실험 때는 compose의 `compare` 프로필로 Fluent Bit 두 개(`fluent-bit-direct` :2021, `fluent-bit-direct-async` :2022)가 더 뜬다. 같은 장비 파일을 읽어 앱의 `POST /ingest/direct`로 보내고, 앱은 Kafka 없이 ClickHouse에 바로 넣는다(`app.compare.enabled=true`일 때만 열린다). ClickHouse 안의 Kafka 테이블 엔진도 `app-logs`를 따로 읽는다. 평소에는 이것들이 꺼져 있다.

수집 서버와 컨슈머가 한 프로세스에 있는 것은 학습 편의를 위한 것이다. 실제 프로젝트에서는 수집 서버와 적재 컨슈머를 별도 서비스로 나눈다. Step 3에서는 앱을 두 개 띄워 같은 그룹의 컨슈머가 파티션을 나눠 갖는 것을 봤다.

### 2.2 데이터 흐름 여섯 구간

로그 한 줄은 여섯 구간을 지난다. 각 구간에서 무엇이 바뀌는지가 이 프로젝트의 핵심 내용이며, `study/02-메시지의-여행.md`가 코드 한 줄씩 따라간다.

| 구간 | 들어오는 것 | 나가는 것 | 담당 |
|---|---|---|---|
| 1. 파일 → Fluent Bit | `.log`/`.txt` 파일의 새 줄 | 레코드 (`log`, `source`=파일 경로, `offset`=파일 내 바이트 위치) | `fluent-bit.conf` `[INPUT] tail` |
| 2. Fluent Bit → HTTP | 레코드 묶음 | JSON 배열 POST, `collected_at` 추가. 2xx가 아니면 재시도 | `fluent-bit.conf` `[OUTPUT] http` |
| 3. HTTP → 수집 서버 | JSON 배열 | `FluentBitRecord` 리스트 | `ingest/FluentBitIngestController` |
| 4. 정규화 | 원문 한 줄 + 경로 | `LogEvent` (device_id 추출, event_time UTC 변환, level/category/message 파싱) | `log/LogNormalizer` |
| 5. 발행 | `LogEvent` | `app-logs` 토픽, 키=device_id, 값=snake_case JSON. 브로커 ack 후 200 응답 | `producer/LogEventProducer` |
| 6. 소비·적재 | 파티션의 메시지 | 그룹마다 따로: 콘솔 로그(log-printer), 레벨별 건수(log-stats, Step 3), ClickHouse `logs.app_logs`에 묶음 INSERT(clickhouse-sink, Step 4) | `consumer/`, `sink/` |

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
| `consumer/` | `@KafkaListener` (log-printer, log-stats) | 소비해서 하는 일이 바뀔 때 |
| `sink/` | ClickHouse 적재 컨슈머(배치 리스너, client-v2 RowBinary, 무한 재시도), `/api/sink/stats` (Step 4). `ClickHouseWriter`는 직접 적재도 함께 쓴다 | 저장소나 적재 방식이 바뀔 때 |
| `direct/` | 비교용 직접 적재 `POST /ingest/direct`(`?async=true`면 비동기 INSERT), `/api/compare/stats` (Step 4+, `app.compare.enabled=true`일 때만) | 적재 경로 비교 실험이 바뀔 때 |
| `config/` | 토픽 선언, ClickHouse 클라이언트 | 토픽 구성이나 저장소 접속이 바뀔 때 |
| `simulator/`, `api/` | 연습·실험용 | 실험 항목이 늘 때 |

설정은 `application.properties` 하나에 세 묶음으로 둔다. `spring.kafka.*`는 Spring Boot가 읽어 Kafka 클라이언트를 만들고, `app.*`는 이 프로젝트가 정의한 키이며, `logging.*`은 로그 레벨이다. 항목마다 주석으로 "왜"를 적어 두었으므로 파일을 직접 여는 것이 가장 빠르다.

### 2.5 지금 어디까지 되어 있는가

Step 4+까지 완료됐다. 2026-10-19에 볼륨을 모두 지운 깨끗한 환경에서 `practice/00`~`08`을 처음부터 따라 해 기대 결과를 재현했다(E절 생성기 실행은 Python이 없어 제외, `practice/README.md` 7절). 코드에 뒤 단계가 들어 있으므로 앞 단계의 실습은 앱을 그 단계의 설정으로 띄운다(`practice/00` 0-3의 단계 인자). 실측값은 `result/step3`, `step4`, `step4plus`, `benchmark-storage.md`, `benchmark-ingest-path.md`에 있고, 처리량 한계는 `kafka-load-test` 브랜치의 측정을 인용했다.

Step 4+에서 같은 로그를 네 경로로 넣어 비교한 결과, Kafka 경유는 INSERT 크기를 장비 수에서 떼어 놓고(직접 적재의 part 47분의 1), 저장소 장애를 장비 쪽에서 떼어 놓고(3분 정지 동안 수집 서버는 계속 200), 지난 로그를 다시 넣을 수 있게 했다(오프셋 되돌리기). 측정 중에 Fluent Bit 쪽 문제 둘도 드러났다. 디스크 버퍼에 청크 몇 개가 남아 재시작 전까지 보내지지 않는 것, 그리고 Fluent Bit 4.2.8이 재시작 뒤 `offset`을 0부터 다시 세어 멱등키가 다른 줄과 겹치는 것이다. 둘째는 중복 제거를 유실로 바꾸므로 대응(에이전트 버전 또는 키 구성)을 계획서의 열린 항목으로 두었다(`study/11` 6절).

아직 없는 것은 셋이다. 재시도 끝에 실패한 메시지를 따로 빼는 DLT와 gzip 전송(Step 5), 복제와 브로커 장애 복구(Step 6), 스트림 집계(Step 7, 선택). 전체 단계 계획은 `plan/00-학습계획서.md`, 진행 표는 루트 `README.md`에 있다.

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
- consumer 항목, 오프셋 커밋, 리밸런스: `study/04-Consumer-깊게.md`. 리밸런스를 직접 재 본 것은 `study/07-파티션과-리밸런스.md`
- ClickHouse의 part와 병합, 테이블 설계, 중복 제거, TTL, 적재 컨슈머: `study/08-ClickHouse-기초.md`, `09-ClickHouse-적재와-멱등키.md`. Grafana 패널 쿼리: `10-대시보드.md`
- `KafkaTemplate`과 `@KafkaListener` 뒤의 Spring 계층: `concepts/02-spring-kafka.md`
- Fluent Bit, Kafka UI, ClickHouse, Grafana가 각각 무엇인지: `concepts/03-주변-도구.md`
- 대외비 생성기로 실제 형식 로그 흘리기: `study/05-OHT-로그-연동.md`
- 체크리스트식 실습과 기대 결과: `study/06-실습-워크북.md`. 같은 실습을 명령 하나씩 따라 하려면 `practice/README.md`
- Kafka를 거치는 것과 ClickHouse에 바로 넣는 것(비동기 INSERT, Kafka 테이블 엔진 포함)의 차이: `study/11-적재-경로-비교.md`, 따라 하기는 `practice/08`
- 각 단계의 실행 절차와 배운 점: `result/step0`, `step1`, `step2`, `step3`, `step4`, `step4plus`
- 실제 로그가 ClickHouse에서 차지하는 크기(LZ4, ZSTD): `result/benchmark-storage.md`. 경로별 part 수·지연·장애 복구: `result/benchmark-ingest-path.md`. 처리량 한계는 `kafka-load-test` 브랜치의 `result/benchmark-*.md`

## 4. 어디서부터 어떻게 공부하는가

### 4.1 사전 지식

Java와 Spring Boot의 기본(`@Component`, 의존성 주입, `application.properties`)은 안다고 가정한다. Docker는 `docker compose up -d`, `docker ps`, `docker logs` 세 명령이면 된다. Kafka는 몰라도 된다.

### 4.2 학습의 기본 단위

이 저장소의 학습은 "읽는다 → 돌린다 → 바꿔 본다 → 말로 설명한다"의 반복이다. 문서만 읽고 넘어가면 남는 것이 적다. 각 장 끝의 확인 질문에 스스로 답하고, 워크북의 체크박스를 채우며 측정값을 빈칸에 적는다. 설정값을 바꾸고 재시작해 Kafka UI와 콘솔이 어떻게 달라지는지 보는 것이 가장 효과가 크다.

### 4.3 추천 경로

처음 보는 사람 기준으로 네 묶음, 총 15시간 안팎이다. 각 묶음은 "읽기 → 실행 → 실습 → 질문"으로 끝낸다.

**묶음 1. 환경을 띄우고 큰 그림을 잡는다 (2시간)**

1. 이 문서 1절과 2절을 읽는다.
2. `study/01`로 설치하고 띄운다. 먼저 `./gradlew test`로 테스트가 통과하는지 보고(docker 없이 돈다), 앱을 띄워 기동 로그에서 `partitions assigned`를 확인한다. PATH에 Java가 없으면 IntelliJ 내장 JDK를 `JAVA_HOME`으로 잡는다.
3. `concepts/01`로 용어를 익힌다.
4. `practice/00`~`02`를 따라 워크북 A절(Step 0)과 B절(Step 1)을 한다. 코드에는 뒤 단계까지 들어 있으므로 앱은 `practice/00` 0-3의 단계 인자를 넣어 띄운다. CLI로 메시지를 주고받고, REST로 한 건 발행해 콘솔에서 소비를 본다.
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

**묶음 4. 늘리고, 저장하고, 비교한다 (4~5시간, Step 3~4+)**

1. `study/07`을 읽고 `practice/06`으로 워크북 G절을 한다. 앱 둘을 띄워 리밸런스를 시각으로 잰다.
2. `study/08`~`10`을 읽고 `practice/07`로 H절을 한다. ClickHouse를 손으로 만진 뒤 적재 컨슈머를 붙이고, ClickHouse를 멈춰 lag이 쌓였다 풀리는 것을 본다.
3. `study/11`을 읽고 `practice/08`로 I절을 한다. 같은 로그를 네 경로로 넣어 part 수, 지연, 장애 복구, 재처리를 비교한다.
4. 질문: "수집 서버가 하나여도 Kafka가 필요한가"에 이 저장소의 숫자로 답한다.

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
