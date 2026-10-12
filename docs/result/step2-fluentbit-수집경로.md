# Step 2. 명세서 스키마 적용 + Fluent Bit 수집 경로 결과

## 만든 것

Step 1의 "REST → Kafka" 구조를 명세서의 "장비 로그 파일 → Fluent Bit → HTTP → 수집 서버 → Kafka" 구조로 바꿨다.

```
docker/devices/dev-0001.log ─┐
docker/devices/dev-0002.log ─┼─ Fluent Bit (tail) ─ HTTP POST ─▶ /ingest/fluentbit ─▶ LogNormalizer ─▶ KafkaTemplate ─▶ app-logs
   ...        (시뮬레이터가 씀)─┘        ↑ 2xx 아니면 재시도                              (스키마 정규화)      key=device_id
```

| 파일 | 역할 |
|---|---|
| `log/LogEvent.java` | 명세서 스키마. `device_id, source, event_time, collected_at, offset, raw` + 파싱 필드 `level, message`. `idempotencyKey()` 제공 |
| `log/LogNormalizer.java` | 원문 한 줄 → `LogEvent`. 파일 경로에서 device_id 추출, 시각 문자열을 UTC `Instant`로 정규화, level 파싱. 실패하면 collected_at으로 대체 |
| `ingest/FluentBitRecord.java` | Fluent Bit이 보내는 JSON 레코드 구조 |
| `ingest/FluentBitIngestController.java` | `POST /ingest/fluentbit`. 묶음을 정규화해 Kafka에 넣고, 브로커 ack를 받은 뒤 200 응답 |
| `simulator/DeviceLogSimulator.java` | 장비 로그 파일 생성기. `POST /api/logs/simulate?lines=N` 또는 `app.simulator.enabled=true`로 연속 생성 |
| `docker/fluent-bit/fluent-bit.conf` | Fluent Bit 파이프라인 설정. 옵션마다 주석 |
| `docker/docker-compose.yml` | `fluent-bit` 서비스 추가 (`./devices` 마운트, `host.docker.internal:8080`으로 전송) |
| `producer/LogGenerator.java` | Kafka 직접 발행 부하 생성기 (스키마만 갱신) |

## 설계에서 눈여겨볼 것

**응답 코드가 전송 보장의 계약이다.** `FluentBitIngestController`는 묶음 안의 모든 메시지에 대해 Kafka 브로커 ack(`acks=all`)를 받은 뒤에야 200을 돌려준다. Kafka가 내려가 있으면 500을 돌려주고, Fluent Bit은 `Retry_Limit False`에 따라 디스크 버퍼에 들고 있다가 재전송한다. 그래서 수집 서버가 죽든 Kafka가 죽든 로그는 장비 쪽 Fluent Bit 버퍼에 남아 있고, 복구되면 흘러온다. 이것이 명세서의 "무손실 중앙 서버 전송"을 구현하는 방법이다. Step 1에서 REST 응답을 ack 없이 202로 바로 돌려준 것과 대비된다. 사람이 보내는 요청은 빠른 응답이 중요하고, 에이전트가 보내는 요청은 정확한 성공/실패 신호가 중요하다.

**at-least-once이므로 멱등키가 필요하다.** 수집 서버가 Kafka에 넣고 나서 200을 보내기 직전에 죽으면, Fluent Bit은 같은 묶음을 다시 보내고 Kafka에는 같은 줄이 두 번 들어간다. 이를 저장소에서 걸러내려면 "같은 줄"을 식별할 키가 필요한데, 그것이 `(device_id, source, offset)`이다. offset은 Fluent Bit tail 입력의 `Offset_Key`가 넣어주는 **파일 내 바이트 위치**라서 에이전트가 몇 번을 다시 읽어도 같은 줄은 같은 값이다. Kafka의 파티션 오프셋과 이름만 같고 전혀 다른 것이므로 혼동하지 않도록 컨슈머 로그에 `file_offset`으로 따로 찍는다.

**수집 서버는 Kafka에 넣고 끝낸다.** 저장소(ClickHouse)를 여기서 직접 쓰지 않는다. 저장소가 느려지거나 점검 중이면 수집 서버 응답이 느려지고, 그러면 1,000대 에이전트가 동시에 재시도하며 폭주한다. Kafka에 넣기만 하면 수집 서버는 항상 밀리초 단위로 응답하고, 저장소 적재 속도는 컨슈머가 자기 페이스로 따라간다. 이것이 명세서가 Kafka를 "완충"이라 부르는 이유다.

**시각 정규화.** 장비는 로컬 시각(`2026-10-12 14:03:21.123`)을 타임존 표기 없이 찍는다. `LogNormalizer`는 `app.ingest.device-zone`(기본 Asia/Seoul)을 가정해 UTC `Instant`로 바꾼다. 명세서 [배경]의 "시각 기준이 제각각"이라는 문제를 푸는 지점이며, 실제 프로젝트에서는 장비 종류별로 타임존과 형식을 설정으로 둬야 한다. 시각을 못 찾으면 버리지 않고 `collected_at`을 대신 넣는다. raw가 그대로 남아 있으므로 나중에 규칙을 고쳐 재처리할 수 있다.

**Fluent Bit 대신 Spring이 정규화하는 이유.** Fluent Bit의 `[FILTER] parser`로 장비 쪽에서 파싱할 수도 있다. 하지만 1,000대 장비의 설정을 바꾸는 것보다 수집 서버 코드를 바꾸는 것이 쉽고, 정규식 버그가 나도 raw가 중앙에 있으면 복구할 수 있다. 에이전트는 최대한 멍청하게(읽어서 보내기만), 중앙은 똑똑하게 두는 것이 운영상 유리하다.

## 실행 절차

```bash
# 0. 인프라 (fluent-bit 컨테이너가 추가됐으므로 다시 up)
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps     # kafka(healthy), kafka-ui, fluent-bit 세 개

# 1. 앱 실행 (IntelliJ 에서 bootRun 또는 메인 클래스 실행. 작업 디렉터리가 프로젝트 루트여야 docker/devices 경로가 맞는다)

# 2. 장비 로그 200줄 생성 → docker/devices/dev-00NN.log 에 기록됨
curl -X POST "http://localhost:8080/api/logs/simulate?lines=200"

# 3. 몇 초 안에 Fluent Bit 이 읽어서 보낸다. 수신/소비 건수 확인
curl http://localhost:8080/api/logs/stats
#   {"receivedFromFluentBit":200,"consumed":200}

# 4. 정규화 결과를 직접 보고 싶으면 (Kafka 를 거쳐 돌아오는 것이 아니라 정규화 결과만 바로 응답)
curl -X POST http://localhost:8080/api/logs -H "Content-Type: application/json" \
  -d "{\"deviceId\":\"dev-0001\",\"raw\":\"2026-10-12 14:03:21.123 [WARN] ECU temperature high: 97C\"}"
```

`http/step2.http`에 같은 요청이 있다.

## 확인해야 할 것

앱 콘솔에서 `Fluent Bit 묶음 수신 N건` DEBUG 로그와 컨슈머의 `[consume #k] p1@37 key=dev-0007 | event_time=2026-10-12T05:03:21.123Z level=WARN file_offset=1842 | ...` 로그를 본다. 같은 `dev-NNNN` 키는 항상 같은 파티션(p0/p1/p2)에 가는지, `event_time`이 로그 파일의 14:03(KST)에서 05:03(UTC)으로 바뀌었는지, 시각이 없는 줄(약 3%)은 `level=null`이고 `event_time`이 `collected_at`과 같은지 본다.

Kafka UI(http://localhost:8085)의 Topics → app-logs → Messages에서 값이 아래처럼 명세서 스키마의 snake_case JSON인지 확인한다.

```json
{"device_id":"dev-0007","source":"/var/log/devices/dev-0007.log","event_time":"2026-10-12T05:03:21.123Z",
 "collected_at":"2026-10-12T05:03:22.004Z","offset":1842,"raw":"2026-10-12 14:03:21.123 [WARN] ECU temperature high: 97C",
 "level":"WARN","message":"ECU temperature high: 97C"}
```

Fluent Bit 자체 지표는 http://localhost:2020/api/v1/metrics 에서 본다. `input.tail.0.records`(읽은 줄 수), `output.http.0.proc_records`(보낸 수), `output.http.0.retries`(재시도 수), `errors`를 확인한다.

## 무손실 실험 (해볼 것)

1. Spring 앱을 끈 채로 `docker compose ... restart fluent-bit` 후, 다른 터미널에서 `docker/devices/dev-0001.log`에 줄을 몇 개 직접 덧붙인다(`echo "2026-10-12 15:00:00.000 [ERROR] manual test" >> docker/devices/dev-0001.log`).
2. `docker logs fluent-bit`에서 connection refused와 retry 메시지를 본다. http://localhost:2020/api/v1/metrics 의 `retries`가 늘어난다.
3. Spring 앱을 띄운다. 몇 초 뒤 쌓였던 줄이 전부 들어오고 `consumed`가 늘어난다.
4. 이번에는 Kafka를 멈춘다(`docker stop kafka`). 로그를 더 쓰면 Spring이 500을 돌려주고 Fluent Bit이 재시도한다. `docker start kafka` 후 복구되는 것을 본다.

이 실험으로 "수집 서버가 죽어도, Kafka가 죽어도 로그는 유실되지 않는다. 대신 늦게 도착한다"를 확인한다.

## 테스트

```bash
./gradlew test
```

`LogNormalizerTest`는 Spring 없이 도는 단위 테스트로, 정규화 규칙(시각 형식 세 가지, WARNING→WARN, 파싱 실패 시 대체, Windows 경로)을 고정한다. 실로그 샘플을 받아 규칙을 바꾸면 이 테스트부터 갱신한다.

## 배운 점

Kafka를 쓰는 이유가 "메시지를 전달하기 위해서"라기보다 "생산자와 소비자의 속도를 분리하기 위해서"라는 것이 이 단계에서 분명해진다. 에이전트는 수집 서버만 보고, 수집 서버는 Kafka만 보고, 적재기는 Kafka만 본다. 어느 하나가 느려지거나 죽어도 나머지는 영향받지 않고, 복구되면 밀린 것을 따라잡는다. 이 분리가 1,000대 규모에서 장애를 국소화하는 장치다.

at-least-once는 "유실 없음"의 대가로 "중복 가능"을 받아들이는 것이고, 그 중복을 처리할 책임은 파이프라인 끝(저장소)에 있다. 명세서가 offset을 멱등키 재료로 못 박은 것은 이 책임을 설계에 미리 넣어둔 것이다. Step 4에서 ClickHouse의 ReplacingMergeTree로 이 키를 실제 사용한다.

## 결정 비교: Fluent Bit → Kafka 직접 vs → HTTP → 수집 서버 → Kafka

Fluent Bit에는 `kafka` 출력 플러그인이 있어 수집 서버 없이 바로 토픽에 쓸 수 있다. 홉이 하나 줄어 지연과 운영 요소가 줄지만, 1,000대 장비가 브로커 주소와 Kafka 프로토콜 포트(9092)에 직접 접근해야 하고, 정규화·인증·유량 제어를 장비마다 설정해야 한다. 망분리 환경에서 장비가 아는 주소는 HTTP 엔드포인트 하나인 편이 보안·운영상 단순하다. 명세서가 HTTP 경로를 택한 이유가 이것이며, 연습 프로젝트도 따른다. 벤치마크 산출물에서는 두 경로의 처리량·지연을 실측해 비교하면 좋은 근거가 된다.
