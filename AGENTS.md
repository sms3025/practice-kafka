# AGENTS.md — 이 저장소에서 작업하는 Codex를 위한 안내

## 프로젝트 목적

Apache Kafka를 처음 배우는 학습자가, 기업 연계 프로젝트 **Log Collector**(OHT 차량 1,000대 이상의 로컬 로그를 Fluent Bit → HTTP → Spring 수집 서버 → Kafka(완충) → ClickHouse → 대시보드로 모으는 시스템)와 같은 구조의 데모를 단계별로 만들며 배우는 저장소다. 코드의 정확성만큼 **"왜 이렇게 짰는지"를 설명하는 것**이 산출물이다. 전체 계획은 `docs/plan/00-학습계획서.md`, 현재 진행 상태는 `README.md`의 진행 상황 표를 본다.

## 절대 규칙: 대외비 자료

기업이 제공한 OHT 로그 생성기 `ohtsim.pyz`, 그 사용법 문서, 생성기가 만든 로그는 **대외비**이며 이 저장소는 GitHub에 공개되어 있다.

- `tools/`, `docker/devices/*`(`.gitkeep` 제외), `*.pyz`, `사용법.md`는 `.gitignore`에 있다. 절대 `git add -f` 하지 않는다.
- 생성 로그의 실제 내용(장비 상태 메시지, MAC/IP, 좌표 등)을 문서·테스트·커밋 메시지·주석에 붙여 넣지 않는다. 형식 설명이 필요하면 **형식만 같은 가짜 문장**을 쓴다 (`src/test/.../LogNormalizerTest.java`의 예시처럼).
- 커밋 전에 `git status`로 tracked 파일을 확인한다.

## 기술 스택과 제약

Java 25(Gradle toolchain + foojay 자동 다운로드), Spring Boot 4.1.x, Spring for Apache Kafka 4.1.x(Jackson 3 → `JacksonJsonSerializer`/`JacksonJsonDeserializer`), Gradle 9.8 Wrapper, Apache Kafka 4.2 KRaft(docker), Fluent Bit 4.2(docker), Kafbat Kafka UI, ClickHouse + Grafana(Step 4 예정). 설정은 YAML이 아니라 **`application.properties`** 하나에 두고 항목마다 주석으로 의미를 적는다. Lombok은 쓰지 않는다(record 사용).

Spring Boot 4 주의: 웹 스타터는 `spring-boot-starter-webmvc`, 직렬화기는 `JacksonJsonSerializer`(옛 `JsonSerializer`는 Jackson 2용). 인터넷의 3.x 자료와 다르다.

## 명령

```bash
docker compose -f docker/docker-compose.yml up -d      # Kafka + Kafka UI(8085) + Fluent Bit(2020)
./gradlew bootRun                                       # 앱 (8080). IntelliJ 실행 시 Working directory = 프로젝트 루트
./gradlew test                                          # LogNormalizerTest(순수 단위) + @EmbeddedKafka 통합 테스트
scripts/ohtsim.cmd --vehicles 3 --seed 1                # OHT 생성기 → docker/devices (tools/ohtsim.pyz 필요)
curl -X POST "localhost:8080/api/logs/simulate?lines=200"   # 연습용 단순 로그 → docker/devices/sim
curl localhost:8080/api/logs/stats
```

## 코드 구조 (패키지 = 바뀌는 이유 단위)

| 패키지 | 책임 | 바뀌는 때 |
|---|---|---|
| `ingest/` | `POST /ingest/fluentbit` 수신, 브로커 ack 후 2xx (at-least-once 계약) | 에이전트가 바뀔 때 |
| `log/` | `LogEvent`(명세서 스키마, snake_case JSON), `LogNormalizer`(형식 3종 파싱, device_id 경로 추출, 시각 UTC 정규화) | 로그 형식이 바뀔 때 |
| `producer/` | `KafkaTemplate` 발행(키=device_id), 부하 생성기 | Kafka 발행 정책이 바뀔 때 |
| `consumer/` | `@KafkaListener` | 적재 대상이 바뀔 때 |
| `simulator/`, `api/` | 연습·실험용 | |

명세서 스키마 필드: `device_id, source, event_time, collected_at, offset, raw` + 파싱 필드 `category, level, message`. 멱등키는 `(device_id, source, offset)`. 이 구조를 바꾸면 `LogNormalizerTest`와 `docs/study/02-메시지의-여행.md`를 함께 고친다.

## 문서 규칙 (코드 변경과 같은 커밋에)

상세 형식·단계별 예정 산출물·완료 체크리스트는 `docs/plan/01-학습자료-제작-방침.md`를 따른다. 저장소 입구 문서는 `docs/README.md`이며 구성도(2.1)와 진행 상태(2.5)를 단계마다 맞춘다.

- `docs/plan/`: 계획서. 단계 순서나 결정이 바뀌면 "결정 사항" 표에 날짜와 근거를 추가.
- `docs/result/stepN-*.md`: 단계가 끝날 때 "만든 것 / 설계에서 눈여겨볼 것 / 실행 절차 / 확인해야 할 것 / 배운 점". 각 단계는 "코드 → docker로 실행 → Kafka UI·로그로 확인 → 문서" 순환.
- `docs/study/`: 처음 보는 사람을 위한 교재. 새 개념·설정이 들어오면 해당 장에 "설명 → 코드 → 실습 → 확인 질문" 형식으로 보강. 06 워크북에 실습 항목 추가.
- `docs/practice/`: 워크북 항목을 같은 번호로 풀어 쓴 따라 하기 안내(PowerShell 명령, 기대 화면). 워크북에 절을 붙이면 여기에 파일을 추가하고, 실측한 출력은 날짜와 함께 적는다.
- `docs/concepts/`: 개념 사전. 새 도구가 들어오면 03에 추가.
- 문체: 한국어 존댓말이 아닌 평서문("~한다"), 산문 위주, 리스트는 병렬 항목에만. 이모지 금지. 설정 항목마다 "왜"를 적는다.

## 다음 단계 (계획서 기준)

Step 3 파티션·컨슈머 그룹·리밸런스 → Step 4 ClickHouse 적재(단독 체험 → 테이블 설계 → 배치 리스너 → Grafana 네 묶음. ReplacingMergeTree 멱등키, TTL) → Step 4+ 적재 경로 비교(직접 적재·비동기 INSERT·Kafka 경유·Kafka 테이블 엔진, 같은 입력으로 part 수·지연·장애 복구 측정) → Step 5 멱등 프로듀서·DLT·압축 → Step 6 3브로커·복제 → Step 7(선택) Streams. 새 단계를 시작하기 전에 사용자에게 결정이 필요한 선택지(예: 파티션 수, 저장소 테이블 설계)를 먼저 묻는다.

## 환경 메모

사용자 PC는 Windows(Docker Desktop, IntelliJ). PATH에 Java는 없지만 IntelliJ 내장 JDK(`C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr`, 25.0.4)가 있어, 터미널에서는 `JAVA_HOME`을 이것으로 잡고 `gradlew.bat`을 돌린다. 툴체인 JDK는 Gradle이 필요하면 자동 설치한다. Fluent Bit은 Windows 바인드 마운트 때문에 `Inotify_Watcher Off`(폴링)로 둔다. 이 저장소가 만들어진 세션에서는 Maven Central에 접근할 수 없어 Spring 연동 코드가 컴파일 검증 없이 작성됐으나, 2026-10-13에 `./gradlew clean test`로 컴파일과 테스트 9건 통과를 확인했다. 2026-10-14~15에는 떠 있던 docker 환경에서 주요 경로(B1~B3, D1~D3, Kafka 정지 C8·D5)를 실행해 확인했고, 그 과정에서 버그 셋을 고쳤다(`docs/practice/README.md` 7절). 깨끗한 환경 재현은 아직이다. 터미널 명령은 PowerShell 기준으로 쓴다. Git Bash는 `docker exec`의 `/opt/...` 경로를 바꿔 버리므로 docker 명령에 쓰지 않는다. Python은 Store 바로가기만 있고 실제로는 설치돼 있지 않다(2026-10-15).
