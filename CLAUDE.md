# CLAUDE.md — 이 저장소에서 작업하는 Claude를 위한 안내

## 프로젝트 목적

Apache Kafka를 처음 배우는 학습자가, 기업 연계 프로젝트 **Log Collector**(OHT 차량 1,000대 이상의 로컬 로그를 Fluent Bit → HTTP → Spring 수집 서버 → Kafka(완충) → ClickHouse → 대시보드로 모으는 시스템)와 같은 구조의 데모를 단계별로 만들며 배우는 저장소다. 코드의 정확성만큼 **"왜 이렇게 짰는지"를 설명하는 것**이 산출물이다. 전체 계획은 `docs/plan/00-학습계획서.md`, 현재 진행 상태는 `README.md`의 진행 상황 표를 본다.

## 절대 규칙: 대외비 자료

기업이 제공한 OHT 로그 생성기 `ohtsim.pyz`, 그 사용법 문서, 생성기가 만든 로그는 **대외비**이며 이 저장소는 GitHub에 공개되어 있다.

- `tools/`, `docker/devices/*`(`.gitkeep` 제외), `*.pyz`, `사용법.md`는 `.gitignore`에 있다. 절대 `git add -f` 하지 않는다.
- 생성 로그의 실제 내용(장비 상태 메시지, MAC/IP, 좌표 등)을 문서·테스트·커밋 메시지·주석에 붙여 넣지 않는다. 형식 설명이 필요하면 **형식만 같은 가짜 문장**을 쓴다 (`src/test/.../LogNormalizerTest.java`의 예시처럼).
- ClickHouse 볼륨(`practice-kafka_clickhouse-data`)과 Kafka 토픽에도 실제 로그가 들어간다. 로컬 docker 볼륨이라 커밋 대상은 아니지만, 화면 캡처나 쿼리 결과를 문서에 붙이지 않는다. 결과 문서에는 크기·개수·시각만 적는다.
- 커밋 전에 `git status`로 tracked 파일을 확인한다.

## 기술 스택과 제약

Java 25(Gradle toolchain + foojay 자동 다운로드), Spring Boot 4.1.x, Spring for Apache Kafka 4.1.x(Jackson 3 → `JacksonJsonSerializer`/`JacksonJsonDeserializer`), Gradle 9.8 Wrapper, Apache Kafka 4.2 KRaft(docker), Fluent Bit 4.2(docker, 실제 4.2.8), Kafbat Kafka UI, ClickHouse 25.8(client-v2 0.10) + Grafana 11.6. 설정은 YAML이 아니라 **`application.properties`** 하나에 두고 항목마다 주석으로 의미를 적는다. Lombok은 쓰지 않는다(record 사용).

Spring Boot 4 주의: 웹 스타터는 `spring-boot-starter-webmvc`, 직렬화기는 `JacksonJsonSerializer`(옛 `JsonSerializer`는 Jackson 2용). 인터넷의 3.x 자료와 다르다.

## 명령

```bash
docker compose -f docker/docker-compose.yml up -d      # Kafka + Kafka UI(8085) + Fluent Bit(2020) + ClickHouse(8123) + Grafana(3000)
./gradlew bootRun                                       # 앱 (8080). IntelliJ 실행 시 Working directory = 프로젝트 루트
./gradlew test                                          # LogNormalizerTest(순수 단위) + @EmbeddedKafka 통합 테스트
scripts/ohtsim.cmd --vehicles 3 --seed 1                # OHT 생성기 → docker/devices (tools/ohtsim.pyz 필요)
curl -X POST "localhost:8080/api/logs/simulate?lines=200"   # 연습용 단순 로그 → docker/devices/sim
curl localhost:8080/api/logs/stats                     # log-printer, log-stats(레벨별) 건수
curl localhost:8080/api/logs/assignments               # 컨슈머 스레드별 파티션 할당 (Step 3)
curl localhost:8080/api/sink/stats                     # ClickHouse 적재 통계와 lag (Step 4)
docker exec clickhouse clickhouse-client --user app --password app   # ClickHouse SQL (PowerShell 에서)
# Step 4+ 적재 경로 비교: 비교용 에이전트 둘(2021, 2022) + 앱 비교 모드 + 비교 SQL
docker compose -f docker/docker-compose.yml --profile compare up -d
./gradlew bootRun --args="--app.compare.enabled=true"
docker exec clickhouse clickhouse-client --user app --password app --queries-file /compare/01-tables.sql   # 02-compare.sql, 99-drop.sql
curl -X POST localhost:8080/api/logs/simulate/start     # 장비 100대 x 초당 60줄 x 600초 (devices, perDevice, seconds)
curl localhost:8080/api/compare/stats                  # 직접 적재 경로의 INSERT 수와 실패(500) 수
```

실습 B~D절은 앱을 `--app.kafka.topics.app-logs.partitions=3 --spring.kafka.listener.concurrency=1 --app.sink.enabled=false`로, G절은 `--app.sink.enabled=false`로 띄운다(코드에 Step 4+까지 있어서, `docs/practice/00-준비.md` 0-3). 초당 수천 줄을 흘릴 때는 `--logging.level.com.practice.kafka.producer=INFO --logging.level.com.practice.kafka.consumer=WARN --logging.level.com.practice.kafka.ingest=INFO`를 붙인다. 줄마다 콘솔에 찍으면 콘솔이 병목이 된다.

## 코드 구조 (패키지 = 바뀌는 이유 단위)

| 패키지 | 책임 | 바뀌는 때 |
|---|---|---|
| `ingest/` | `POST /ingest/fluentbit` 수신, 브로커 ack 후 2xx (at-least-once 계약) | 에이전트가 바뀔 때 |
| `log/` | `LogEvent`(명세서 스키마, snake_case JSON), `LogNormalizer`(형식 3종 파싱, device_id 경로 추출, 시각 UTC 정규화) | 로그 형식이 바뀔 때 |
| `producer/` | `KafkaTemplate` 발행(키=device_id), 부하 생성기 | Kafka 발행 정책이 바뀔 때 |
| `consumer/` | `@KafkaListener` (log-printer 콘솔, log-stats 레벨별 건수) | 소비해서 하는 일이 바뀔 때 |
| `sink/` | Kafka → ClickHouse 적재 컨슈머(배치 리스너, client-v2 RowBinaryWithDefaults, 무한 재시도). 컬럼 목록 `ClickHouseWriter.COLUMNS`는 `docker/clickhouse/init/01-schema.sql`, `docker/clickhouse/compare/01-tables.sql`의 큐 테이블과 같아야 한다(`ClickHouseWriterColumnsTest`) | 저장소나 적재 방식이 바뀔 때 |
| `direct/` | Step 4+ 비교용 직접 적재 `POST /ingest/direct`(`?async=true`면 `async_insert=1, wait_for_async_insert=1`), `/api/compare/stats`. `app.compare.enabled=true`일 때만 생긴다 | 비교 실험이 바뀔 때 |
| `config/` | 토픽 선언, ClickHouse 클라이언트(적재 컨슈머와 직접 적재가 함께 씀) | 토픽이나 저장소 접속이 바뀔 때 |
| `simulator/`, `api/` | 연습·실험용(`/api/logs/simulate/start`로 장비 N대 × 초당 M줄 부하) | |

명세서 스키마 필드: `device_id, source, event_time, collected_at, offset, raw` + 파싱 필드 `category, level, message`. 멱등키는 `(device_id, source, offset)`. 이 구조를 바꾸면 `LogNormalizerTest`와 `docs/study/02-메시지의-여행.md`를 함께 고친다. 주의: Fluent Bit 4.2.8은 재시작 뒤 이어 읽는 줄의 `offset`을 0부터 다시 센다(5.1.4는 정상, 2026-10-19 최소 실험으로 확인). 그러면 멱등키가 다른 줄과 겹쳐 ReplacingMergeTree가 서로 다른 줄을 지운다. 대응(버전 업 또는 키 변경)은 사용자 결정 대기 중이다(`docs/study/11-적재-경로-비교.md` 6절, 계획서 열린 항목).

## 문서 규칙 (코드 변경과 같은 커밋에)

상세 형식·단계별 예정 산출물·완료 체크리스트는 `docs/plan/01-학습자료-제작-방침.md`를 따른다. 저장소 입구 문서는 `docs/README.md`이며 구성도(2.1)와 진행 상태(2.5)를 단계마다 맞춘다.

- `docs/plan/`: 계획서. 단계 순서나 결정이 바뀌면 "결정 사항" 표에 날짜와 근거를 추가.
- `docs/result/stepN-*.md`: 단계가 끝날 때 "만든 것 / 설계에서 눈여겨볼 것 / 실행 절차 / 확인해야 할 것 / 배운 점". 각 단계는 "코드 → docker로 실행 → Kafka UI·로그로 확인 → 문서" 순환.
- `docs/study/`: 처음 보는 사람을 위한 교재. 새 개념·설정이 들어오면 해당 장에 "설명 → 코드 → 실습 → 확인 질문" 형식으로 보강. 06 워크북에 실습 항목 추가.
- `docs/practice/`: 워크북 항목을 같은 번호로 풀어 쓴 따라 하기 안내(PowerShell 명령, 기대 화면). 워크북에 절을 붙이면 여기에 파일을 추가하고, 실측한 출력은 날짜와 함께 적는다.
- `docs/concepts/`: 개념 사전. 새 도구가 들어오면 03에 추가.
- 문체: 한국어 존댓말이 아닌 평서문("~한다"), 산문 위주, 리스트는 병렬 항목에만. 이모지 금지. 설정 항목마다 "왜"를 적는다.

## 다음 단계 (계획서 기준)

Step 0~4+는 2026-10-19에 깨끗한 환경(`down -v`)에서 `docs/practice/00`~`08`을 재현해 완료로 올렸다(E절 생성기 실행은 Python이 없어 제외). 다음은 Fluent Bit `Offset_Key` 버그 대응 결정(사용자) → Step 5 멱등 프로듀서·DLT·gzip 전송(디스크 버퍼에 청크가 남는 문제의 원인 확인 포함) → Step 6 3브로커·복제 → Step 7(선택) Streams. 새 단계를 시작하기 전에 사용자에게 결정이 필요한 선택지(예: 파티션 수, 저장소 테이블 설계)를 먼저 묻는다. 벤치마크는 중복 측정하지 않는다. `kafka-load-test` 브랜치(부하·병목 측정 전용, main에 합치지 않음)에서 이미 잰 값(Kafka 처리량, 전송 구간별 병목, 적재 처리량)은 인용하고 새로 잴 것만 잰다.

## 환경 메모

사용자 PC는 Windows(Docker Desktop, IntelliJ). PATH에 Java는 없지만 IntelliJ 내장 JDK(`C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr`, 25.0.4)가 있어, 터미널에서는 `JAVA_HOME`을 이것으로 잡고 `gradlew.bat`을 돌린다. 툴체인 JDK는 Gradle이 필요하면 자동 설치한다. Fluent Bit은 Windows 바인드 마운트 때문에 `Inotify_Watcher Off`(폴링)로 둔다. 이 저장소가 만들어진 세션에서는 Maven Central에 접근할 수 없어 Spring 연동 코드가 컴파일 검증 없이 작성됐으나, 2026-10-13에 `./gradlew clean test`로 컴파일과 테스트 9건 통과를 확인했다. 2026-10-14~15에는 떠 있던 docker 환경에서 주요 경로(B1~B3, D1~D3, Kafka 정지 C8·D5)를 실행해 확인했고, 그 과정에서 버그 셋을 고쳤다(`docs/practice/README.md` 7절). 2026-10-19에 깨끗한 환경 재현을 마쳤다(테스트 16건). 비교 프로필로 띄운 뒤 지울 때는 `docker compose ... --profile compare down -v`라야 비교 에이전트와 그 볼륨까지 지워진다. 터미널 명령은 PowerShell 기준으로 쓴다. Git Bash는 `docker exec`의 `/opt/...` 경로를 바꿔 버리므로 docker 명령에 쓰지 않는다. Python은 Store 바로가기만 있고 실제로는 설치돼 있지 않다(2026-10-15). 그래서 생성기 대신 `tools/OHT-0001`(이미 생성된 1대분)을 `docker/devices/`에 복사해 실제 경로로 흘린다. 앱 두 개(Step 3)는 `java -jar build/libs/...jar --server.port=8081`로 띄우고, 정상 종료는 `POST /actuator/shutdown`(PowerShell은 `-ContentType application/json` 필요)이다. `Stop-Process`는 강제 종료다. Grafana는 첫 기동 때 ClickHouse 플러그인을 인터넷에서 받는다.
