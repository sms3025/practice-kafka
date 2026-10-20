# practice-kafka

Apache Kafka를 처음 배우는 입장에서, 기업 연계 프로젝트(1,000대 이상 디바이스 통합 로그 수집 Log Collector)와 같은 구조의 데모를 단계별로 만들어 보며 학습하는 프로젝트입니다.

```
장비 로그 파일 ─ Fluent Bit ─ HTTP ─▶ Spring 수집 서버 ─▶ Kafka ─▶ 컨슈머 ─▶ ClickHouse ─▶ Grafana
```

## 스택

Java 25 · Spring Boot 4.1 · Spring for Apache Kafka 4.1 · Gradle 9.8 · Apache Kafka 4.2 (KRaft) · Fluent Bit 4.2 · Docker Compose · Kafbat Kafka UI · ClickHouse + Grafana (Step 4~)

## 빠른 시작

```bash
# 1. Kafka + Kafka UI + Fluent Bit + ClickHouse + Grafana 기동
docker compose -f docker/docker-compose.yml up -d

# 2. 앱 실행 (JDK 25가 없으면 Gradle이 자동으로 내려받는다)
./gradlew bootRun            # Windows: gradlew.bat bootRun  (IntelliJ 실행도 가능)

# 3. 장비 로그 200줄 생성 → Fluent Bit 이 읽어 수집 서버로 → Kafka
curl -X POST "http://localhost:8080/api/logs/simulate?lines=200"
curl http://localhost:8080/api/logs/stats

# (Step 4+) 적재 경로 비교: 직접 적재용 에이전트 둘을 더 띄우고 앱을 비교 모드로
docker compose -f docker/docker-compose.yml --profile compare up -d
./gradlew bootRun --args="--app.compare.enabled=true"
```

Kafka UI: http://localhost:8085 · Fluent Bit 지표: http://localhost:2020/api/v1/metrics (버퍼: `/api/v1/storage`) · ClickHouse SQL 창: http://localhost:8123/play (app/app) · Grafana: http://localhost:3000 · 적재 상태: http://localhost:8080/api/sink/stats · 경로 비교: http://localhost:8080/api/compare/stats

## 사전 요구사항

- Docker Desktop
- JDK 17 이상 (Gradle 자체를 실행하는 용도. 컴파일/실행용 JDK 25는 Gradle이 `~/.gradle/jdks`에 자동 설치)
- Git
- Python 3.8 이상 (기업 제공 OHT 로그 생성기를 돌릴 때만. 생성기와 생성 로그는 **대외비**이며 저장소에 포함하지 않는다. `docs/study/05-OHT-로그-연동.md`)

## 문서

| 경로 | 내용 |
|---|---|
| [docs/README.md](docs/README.md) | **처음 읽는 한 장.** 아키텍처(실행 구성, 데이터 흐름, 스키마, 코드 구조), 문서 지도, 어디서부터 어떻게 공부할지 |
| [docs/architecture.html](docs/architecture.html) | 위 아키텍처를 한 화면에 그린 구성도. 브라우저로 연다 (실행 구성, 여섯 구간 데이터 흐름, 스키마, 설계 결정, 단계 현황) |
| [docs/plan/00-학습계획서.md](docs/plan/00-학습계획서.md) | 전체 단계 계획, 시나리오, 기술 선택 이유 |
| [docs/plan/01-학습자료-제작-방침.md](docs/plan/01-학습자료-제작-방침.md) | 앞으로 학습자료를 어떤 순환·형식으로 만들지, 단계별 예정 산출물, 완료 체크리스트 |
| [docs/study/](docs/study/) | **학습 교재.** 처음 보는 사람 기준으로 구조, 코드 흐름, 설정, 실습을 순서대로 설명. `00-학습-가이드.md`부터 |
| [docs/practice/](docs/practice/README.md) | **따라 하는 실습.** 워크북 항목을 터미널, 명령(PowerShell), 기대 화면 수준으로 풀어 씀. 준비부터 OHT 생성기까지 여섯 파일 |
| [docs/concepts/](docs/concepts/) | 개념 사전 (Kafka 기본, Spring Kafka 구조, 주변 도구) |
| [Kafka 기초 그림 학습 자료](docs/concepts/01-kafka-basics.html) | 가상 로그 한 줄의 이동, 파티션·그룹, 커밋과 재처리, 완충을 직접 조작하며 이해한다. 브라우저로 연다 |
| [리스너 그림 학습 자료](docs/concepts/01-kafka-listeners.html) | 첫 접속 주소(bootstrap)와 안내받는 주소(advertised)의 접속 흐름을 단계별로 따라가고, 설정을 바꾸면 어디서 끊기는지 본다. 브라우저로 연다 |
| [docs/result/](docs/result/) | 단계별 구현 설명 · 실행 확인 절차 · 배운 점 |

## 진행 상황

각 단계는 "결정 질문 → 코드 → docker 실행 → Kafka UI·로그·지표로 확인 → 문서 → 커밋"을 돈다. 실습은 [docs/study/06-실습-워크북.md](docs/study/06-실습-워크북.md)의 절 단위로 하되 따라 하는 절차는 [docs/practice/](docs/practice/README.md)를 보고, 단계 기록은 `docs/result/`에 남긴다. 상태의 뜻은 다음과 같다. **완료**는 [docs/plan/01](docs/plan/01-학습자료-제작-방침.md) 6절의 체크리스트(테스트 통과, 깨끗한 환경에서 실행 절차 재현)를 통과한 것이고, **구현 완료, 실행 검증 전**은 코드와 문서는 있으나 아직 돌려서 확인하지 않은 것이다. 2026-10-19에 볼륨을 모두 지운 깨끗한 환경에서 `docs/practice/00`~`08`을 처음부터 따라 해(E절 생성기 실행은 이 PC에 Python이 없어 제외) Step 1~4+를 완료로 올렸다. 테스트는 16건 통과다. 코드에 Step 4+까지 들어 있으므로 앞 단계의 실습은 앱을 그 단계의 설정으로 띄운다([practice/00](docs/practice/00-준비.md) 0-3의 단계 인자). Step 4+ 측정 중에 Fluent Bit 4.2.8이 재시작 뒤 `offset`을 0부터 다시 세어 멱등키가 겹치는 문제를 찾았고, 대응은 계획서의 열린 항목이다([study/11](docs/study/11-적재-경로-비교.md) 6절).

| 단계 | 내용 | 워크북 | 기록 | 상태 |
|---|---|---|---|---|
| Step 0 | 단일 브로커(KRaft) + Kafka UI, CLI로 토픽·메시지 실습 | A | [result/step0](docs/result/step0-환경구성.md) | 완료 |
| Step 1 | `KafkaTemplate` / `@KafkaListener`, JSON 직렬화, 키와 파티션 | B, C | [result/step1](docs/result/step1-producer-consumer.md), [study/03](docs/study/03-Producer-깊게.md), [04](docs/study/04-Consumer-깊게.md) | 완료 (10-19) |
| Step 2 | 명세서 스키마, Fluent Bit → HTTP → 수집 서버 → Kafka, at-least-once 계약 | D | [result/step2](docs/result/step2-fluentbit-수집경로.md), [study/02](docs/study/02-메시지의-여행.md) | 완료 (10-19) |
| Step 2+ | OHT 생성기(대외비) 연동, 로그 형식 3종 파서, `docs/study` 교재 | E, F | [study/05](docs/study/05-OHT-로그-연동.md), [06](docs/study/06-실습-워크북.md) | 완료 (10-19, 생성기 실행은 Python이 없어 미재현. 생성된 로그는 H6·I에서 실제 경로로 확인) |
| Step 3 | 파티션과 컨슈머 그룹, `concurrency`, 앱 2개 기동, 리밸런스 | G | [result/step3](docs/result/step3-파티션-컨슈머그룹.md), [study/07](docs/study/07-파티션과-리밸런스.md) | 완료 (10-19) |
| Step 4 | ClickHouse 단독 체험(part와 병합), 테이블 설계와 멱등키 중복 제거, TTL, Spring 배치 적재, Grafana, 저장 벤치마크 | H | [result/step4](docs/result/step4-clickhouse-적재.md), [benchmark-storage](docs/result/benchmark-storage.md), [study/08](docs/study/08-ClickHouse-기초.md), [09](docs/study/09-ClickHouse-적재와-멱등키.md), [10](docs/study/10-대시보드.md) | 완료 (10-19) |
| Step 4+ | 적재 경로 비교: 직접 적재, 비동기 INSERT, Kafka 경유, Kafka 테이블 엔진. 평시 부하, ClickHouse 장애, 재처리 | I | [result/step4plus](docs/result/step4plus-적재경로-비교.md), [benchmark-ingest-path](docs/result/benchmark-ingest-path.md), [study/11](docs/study/11-적재-경로-비교.md), [practice/08](docs/practice/08-적재-경로-비교.md) | 완료 (10-19) |
| Step 5 | 재시도와 DLT, 수동 ack, 멱등 프로듀서, 전송 보장 수준, gzip 전송 벤치마크 | J | `result/step5`, `benchmark-transport`, `study/12`, `13` | 예정 |
| Step 6 | 3브로커 클러스터, 복제, 브로커 장애 실험 | K | `result/step6`, `study/14` | 예정 |
| Step 7 | Kafka Streams 집계, 알림 토픽 | L | `result/step7`, `study/15` | 선택 |
| 마무리 | 연습 결과를 기업 프로젝트의 결정으로 연결, 벤치마크 종합 | F 확장 | `result/summary` | 예정 |

예정 파일명은 [docs/plan/01](docs/plan/01-학습자료-제작-방침.md) 4절 산출물 표를 따르며, 그 표가 바뀌면 여기도 고친다.
