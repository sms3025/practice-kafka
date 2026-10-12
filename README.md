# practice-kafka

Apache Kafka를 처음 배우는 입장에서, 기업 연계 프로젝트(1,000대 이상 디바이스 통합 로그 수집 Log Collector)와 같은 구조의 데모를 단계별로 만들어 보며 학습하는 프로젝트입니다.

```
장비 로그 파일 ─ Fluent Bit ─ HTTP ─▶ Spring 수집 서버 ─▶ Kafka ─▶ 컨슈머 ─▶ ClickHouse ─▶ Grafana
```

## 스택

Java 25 · Spring Boot 4.1 · Spring for Apache Kafka 4.1 · Gradle 9.8 · Apache Kafka 4.2 (KRaft) · Fluent Bit 4.2 · Docker Compose · Kafbat Kafka UI · ClickHouse + Grafana (Step 4~)

## 빠른 시작

```bash
# 1. Kafka + Kafka UI + Fluent Bit 기동
docker compose -f docker/docker-compose.yml up -d

# 2. 앱 실행 (JDK 25가 없으면 Gradle이 자동으로 내려받는다)
./gradlew bootRun            # Windows: gradlew.bat bootRun  (IntelliJ 실행도 가능)

# 3. 장비 로그 200줄 생성 → Fluent Bit 이 읽어 수집 서버로 → Kafka
curl -X POST "http://localhost:8080/api/logs/simulate?lines=200"
curl http://localhost:8080/api/logs/stats
```

Kafka UI: http://localhost:8085 · Fluent Bit 지표: http://localhost:2020/api/v1/metrics

## 사전 요구사항

- Docker Desktop
- JDK 17 이상 (Gradle 자체를 실행하는 용도. 컴파일/실행용 JDK 25는 Gradle이 `~/.gradle/jdks`에 자동 설치)
- Git

## 문서

| 경로 | 내용 |
|---|---|
| [docs/plan/00-학습계획서.md](docs/plan/00-학습계획서.md) | 전체 단계 계획, 시나리오, 기술 선택 이유 |
| [docs/concepts/](docs/concepts/) | 개념 정리 (Kafka 기본, Spring Kafka 구조, ...) |
| [docs/result/](docs/result/) | 단계별 구현 설명 · 실행 확인 절차 · 배운 점 |

## 진행 상황

| 단계 | 내용 | 상태 |
|---|---|---|
| Step 0 | 단일 브로커 환경 + CLI 실습 | 완료 |
| Step 1 | Producer / Consumer 기본, JSON 직렬화 | 완료 |
| Step 2 | 명세서 스키마 적용, Fluent Bit → HTTP → 수집 서버 → Kafka | 완료 |
| Step 3 | 파티션과 컨슈머 그룹, 리밸런스 | 예정 |
| Step 4 | ClickHouse 적재 + Grafana | 예정 |
| Step 5 | 멱등 프로듀서, 재시도, Dead Letter Topic, 압축 전송 | 예정 |
| Step 6 | 3브로커 클러스터, 복제와 장애 실험 | 예정 |
| Step 7 | (선택) Kafka Streams 집계 / 이상 탐지 | 예정 |
