# 05. OHT 로그 생성기 연동: 실제 형식의 로그를 파이프라인에 흘려보내기

## 0. 먼저, 대외비 규칙

기업이 제공한 로그 생성기(`ohtsim.pyz`)와 그 사용법 문서, 그리고 생성기가 만든 로그는 **모두 대외비**다. 제공받은 목적(이 프로젝트) 외 사용, 외부 유출, 복제·재배포, GitHub 같은 공개 저장소 업로드가 금지되며 생성 로그 일부를 캡처해 올리는 것도 포함된다. 프로젝트가 끝나면 파일과 로그를 삭제해야 한다.

이 저장소는 GitHub에 공개되어 있다. 따라서 아래 규칙을 지킨다.

- `ohtsim.pyz`는 프로젝트의 `tools/` 폴더에 두고, 사용법 문서는 저장소 밖(예: 바탕화면)에 둔다. `.gitignore`가 `*.pyz`, `/tools/`, `사용법.md`를 막는다.
- 생성 로그는 `docker/devices/` 아래에 쓴다. `.gitignore`가 `/docker/devices/*`(`.gitkeep` 제외)를 막는다.
- 문서, 테스트, 커밋 메시지, 이슈에 생성 로그를 그대로 붙이지 않는다. 형식 설명이 필요하면 이 문서처럼 **형식만 같은 가짜 내용**을 쓴다.
- push 전에 `git status`로 tracked 파일 목록을 확인한다. `git add -f`는 쓰지 않는다.

## 1. 생성기가 만드는 것

생성기는 OHT 차량의 온보드 로그를 실시간(또는 배속·일괄)으로 만든다. 차량마다 폴더가 하나 생기고, 그 안에 두 계열의 로거가 각자 파일을 쓴다. 파일은 1시간 단위로 바뀐다.

```
docker/devices/
  OHT-0001/
    OHTHandlerLogger/
      SEQ_INFO/20260105_09_SEQ_INFO.txt         주행 시퀀스 (가장 많음)
      API_CALL_INFO/20260105_09_API_CALL_INFO.txt
      PARTS_INFO/20260105_09_PARTS_INFO.txt     드묾. 기동 시 설정값 등
      WMX/20260105_09_WMX.txt                   드묾
    UILogger/
      APP_20260105_09.log                       상위 시스템과의 통신
      OHT_20260105_09.log                       상태 조회 (가장 많음)
      OPS_20260105_09.log                       위치/경로
      NET_20260105_09.log                       Wi-Fi 상태·KPI
      IO_20260105_09.log                        드묾
  OHT-0002/
  ...
```

줄 형식은 계열마다 다르다. 아래는 형식만 흉내 낸 예시다.

```
OHTHandlerLogger:  [2026-01-05 09:12:30.1234],[SEQ_INFO],[INFORMATION],[316710],[msg => example body]
UILogger:          2026-01-05 09:12:30,123 [1] INFO APP - example body
```

두 형식의 차이가 바로 명세서 [배경]의 "포맷·시각 기준이 제각각"이다. 소수점 자리수(4자리 vs 3자리), 구분자(`.` vs `,`), 레벨 표기(`INFORMATION` vs `INFO`), 대괄호 유무가 모두 다르다. 파일은 UTF-8, 줄바꿈은 CRLF다. 생성량은 `peak` 프로파일에서 차량 1대당 약 60줄/s, 시간당 36MB다. 1,000대면 시간당 36GB이므로 로컬 실습에서는 1~10대만 쓴다.

## 2. 파이프라인이 이 형식을 다루는 방식

### Fluent Bit: 중첩 폴더와 시간별 파일

```
Path  /var/log/devices/*/UILogger/*.log, /var/log/devices/*/OHTHandlerLogger/*/*.txt, /var/log/devices/sim/*.log
```

와일드카드가 폴더 단계마다 들어가 모든 차량의 모든 계열 파일을 잡는다. 정시가 지나 새 파일이 생기면 `Refresh_Interval 5`초 안에 발견해 처음부터(`Read_from_Head`) 읽는다. 지난 시간 파일은 더 이상 안 바뀌므로 자연히 조용해진다.

### LogNormalizer: 두 형식 + 폴더명에서 device_id

02장에서 본 대로 정규식 HANDLER와 UILOGGER가 두 형식을 각각 맡는다. `device_id`는 파일명이 아니라 경로의 `OHT-0001` 폴더명에서 뽑는다(`app.ingest.device-id-pattern`). `category`에는 HANDLER 계열이면 `SEQ_INFO` 같은 계열명, UILogger 계열이면 `APP`/`OHT`/`OPS`/`NET`/`IO` 로거명이 들어간다. 레벨은 `INFORMATION`→`INFO`로 통일한다.

이 파서는 생성기가 20분치 3대분으로 만든 약 10만 줄 전부(100%)를 파싱했다. 실로그(마스킹본)를 받으면 같은 방식으로 파싱률을 재고 정규식을 보강한다. `LogNormalizerTest`의 가짜 예시도 그에 맞춰 늘린다.

### 메시지 키와 파티션

키는 `OHT-0001` 같은 device_id다. 한 차량의 9개 파일에서 나온 줄이 모두 같은 파티션에 들어가므로, 컨슈머는 **차량 단위로 시간 순서**를 본다(파일 간 순서는 Fluent Bit이 읽은 순서라 밀리초 단위로 섞일 수 있다. 정렬이 필요하면 `event_time`으로 한다).

## 3. 실행

### 준비

1. `ohtsim.pyz`를 프로젝트의 `tools/` 폴더에 복사한다 (`tools/ohtsim.pyz`).
2. Python 3.8 이상이 `python`(Windows) 또는 `python3`(Linux/macOS) 명령으로 실행되는지 확인한다.
3. 인프라와 Spring 앱을 띄운다 (01장).

### 실시간 3대

```
scripts\ohtsim.cmd --vehicles 3 --seed 1            (Windows)
scripts/ohtsim.sh  --vehicles 3 --seed 1            (Linux/macOS)
```

스크립트는 `python tools/ohtsim.pyz --out docker/devices --retain-hours 2 <인자>`를 실행한다. `--retain-hours 2`는 2시간 지난 파일을 자동 삭제해 디스크를 보호한다. `--seed 1`은 같은 차량 구성을 재현한다(재시작해도 같은 차량으로 이어가려면 같은 seed를 준다). 멈출 때는 Ctrl+C.

몇 초 뒤 앱 콘솔에 `[consume #...] p1@... key=OHT-0001 | event_time=... level=INFO file_offset=... | ...`가 초당 수백 줄 흐른다. 콘솔이 너무 빠르면 `logging.level.com.practice.kafka.consumer=WARN`으로 낮추고 `/api/logs/stats`로 센다.

### 10배속, 또는 과거 구간 일괄 생성

```
scripts\ohtsim.cmd --vehicles 3 --seed 1 --speed 10
scripts\ohtsim.cmd --vehicles 10 --seed 1 --speed 0 --duration 10m --start 2026-01-05T09:00:00
```

`--speed 0`은 기다리지 않고 최대 속도로 만든다(부하 테스트용, `--duration` 필수). 10대 10분치면 약 36만 줄이 몇 초 만에 생기고, Fluent Bit이 이를 읽어 보내는 동안 수집 서버와 Kafka가 얼마나 받아내는지 볼 수 있다. `--start`로 과거 구간을 만들 때는 **빈 폴더**에 써야 한다. 이미 있는 시간대 파일에 겹쳐 쓰면 한 파일 안에서 시각 순서가 섞인다.

### 확인 포인트

- Kafka UI → Topics → app-logs → Messages: 값의 `device_id`가 `OHT-000N`, `category`가 `SEQ_INFO`/`APP` 등, `event_time`이 UTC(`Z`)로 바뀌어 있는지.
- 같은 `device_id`가 항상 같은 파티션인지.
- Kafka UI → Consumers → log-printer: lag이 0에 수렴하는지. `--speed 0` 직후에는 수만까지 올랐다가 내려와야 정상이다.
- http://localhost:2020/api/v1/metrics: `input.tail.0.records`와 `output.http.0.proc_records`가 같이 늘고 `retries`가 0인지.
- `/api/logs/stats`의 `receivedFromFluentBit`이 생성기가 종료 시 보여주는 누적 줄 수와 같은지 (약간의 지연 후).

### 정리

```
docker compose -f docker/docker-compose.yml down -v     # Fluent Bit 의 읽기 위치(DB)와 Kafka 데이터까지 삭제
rmdir /s /q docker\devices\OHT-0001  ...                 # 생성 로그 삭제 (Windows)
```

`down -v` 없이 생성 로그만 지우고 다시 만들면, Fluent Bit은 "이미 읽은 파일"로 기억한 위치보다 파일이 작아진 것을 보고 처음부터 다시 읽거나 건너뛸 수 있다. 실험을 깨끗이 반복하려면 둘 다 지운다.

## 4. 이 단계에서 보이는 것

연습용 단순 로그(`DeviceLogSimulator`)로는 안 보이던 문제가 실제 형식에서 드러난다. CRLF의 `\r`이 본문 끝에 붙는 것, 소수점 4자리 시각, `INFORMATION` 같은 비표준 레벨, 장비 ID가 파일명에 없는 것. 로그 파이프라인 작업의 절반은 이런 "지저분함"을 정규화에서 흡수하는 일이고, 그래서 raw를 그대로 보존하고 파서를 테스트로 고정하는 습관이 중요하다.

생성량도 체감된다. 3대만 돌려도 초당 180줄이고 1,000대면 6만 줄/s다. 컨슈머가 콘솔에 한 줄씩 찍는 지금 구조로는 턱없이 부족하며, Step 4에서 배치 리스너로 수천 건씩 묶어 ClickHouse에 넣는 이유가 여기서 나온다.

## 확인 질문

1. 생성기를 `--seed` 없이 재시작하면 같은 `OHT-0001` 폴더에 "다른 차량"의 로그가 섞인다고 사용법에 적혀 있다. 파이프라인 입장에서 무엇이 달라지고, 멱등키는 영향을 받는가?
2. 정시에 파일이 바뀔 때 Fluent Bit이 새 파일을 발견하기까지 최대 몇 초가 걸리는가? 그 사이의 줄은 유실되는가?
3. `PARTS_INFO`처럼 드물게 기록되는 파일은 해당 시간에 내용이 없으면 생기지 않는다. 이것이 Fluent Bit이나 파서에 문제를 일으키는가?
