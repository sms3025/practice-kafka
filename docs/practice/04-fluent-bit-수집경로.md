# 04. Fluent Bit 수집 경로 (워크북 D절, Step 2)

지금까지는 앱이 스스로 메시지를 만들어 Kafka에 넣었다. 이제 실제 구조대로, 장비가 로그 파일에 줄을 쓰면 Fluent Bit이 읽어 HTTP로 수집 서버에 보내고 수집 서버가 Kafka에 넣는다. 이 경로의 핵심은 장애가 나도 로그가 유실되지 않는 것이며, D4와 D5에서 일부러 장애를 넣어 확인한다.

```
docker/devices/sim/dev-00NN.log ─ Fluent Bit(tail) ─ HTTP POST ─▶ /ingest/fluentbit ─▶ Kafka ─▶ 컨슈머
        (DeviceLogSimulator 가 씀)     (컨테이너)                   (앱, ack 받고 200)
```

시작 전 상태: 03 파일을 마치고 설정을 되돌려 앱을 재시작했다. 컨테이너 셋과 앱이 떠 있다. 앱을 재시작했으므로 `/api/logs/stats`는 0에서 시작한다. 컨테이너 셋 중 fluent-bit은 01 파일에서부터 계속 떠 있었고, `docker/devices/sim`이 비어 있어 지금까지 한 줄도 보내지 않았다.

## D1. 연습용 로그 200줄 흘리기

**왜 하는가.** 파일에서 컨슈머까지 전 구간이 이어지는 것을 본다. `DeviceLogSimulator`는 장비 20대 몫의 파일 `dev-0001.log` ~ `dev-0020.log`에 줄을 덧붙인다. `docker/devices`는 fluent-bit 컨테이너에 마운트되어 있어 여기 쓴 줄을 Fluent Bit이 읽는다.

**할 것.**

```powershell
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate?lines=200"
Start-Sleep 8
Invoke-RestMethod http://localhost:8080/api/logs/stats
Get-ChildItem docker\devices\sim
Get-Content docker\devices\sim\dev-0001.log -TotalCount 3
```

**보여야 하는 것.** 2026-10-14 실측으로 8초 뒤 stats가 `receivedFromFluentBit 200`, `consumed 200`이었다. `docker\devices\sim`에 `dev-0001.log`부터 파일이 생기고, 파일 안에는 `2026-10-15 14:03:21.123 [WARN] BATTERY voltage=...` 같은 줄이 있다.

앱 콘솔에는 수집 서버의 DEBUG 로그 `Fluent Bit 묶음 수신 N건 (누적 M)`이 여러 번 찍힌다. 실측에서는 200줄이 20번의 요청으로 나뉘어 들어왔다. 요청 하나가 장비 파일 하나에 대응한다. Fluent Bit의 태그(`Tag device.*`)에 파일 경로가 들어가 파일마다 따로 묶이기 때문이다. 그 아래로 consume 줄이 이어진다. `file_offset`은 이제 -1이 아니라 파일 안의 바이트 위치다.

**안 될 때.** stats가 계속 0이면 앞에서부터 확인한다. `docker exec fluent-bit ls /var/log/devices/sim`으로 컨테이너 안에서 파일이 보이는지, `docker logs --tail 20 fluent-bit`에 오류가 있는지 본다. 파일이 `docker\devices\sim`이 아닌 다른 곳에 생겼다면 앱의 Working directory가 프로젝트 루트가 아니다(00 파일 0-3).

**생각해 볼 것.** 장비가 1,000대면 Fluent Bit은 1초(`Flush 1`)마다 요청을 몇 번 보내게 될까? 실제 운영에서는 Fluent Bit이 장비마다 하나씩 따로 돈다. 그때 수집 서버가 받는 요청 수는 어떻게 되는가?

## D2. 시각이 없는 줄은 어떻게 되는가

**왜 하는가.** 시뮬레이터는 일부러 약 3%의 줄에서 시각을 뺀다. 형식이 깨진 줄을 버리지 않고 어떻게든 저장하는 정규화 규칙(`LogNormalizer`의 마지막 분기)을 확인한다.

**할 것.** 앱 콘솔에서 Ctrl+F로 `level=null`을 찾는다. 200줄이면 보통 서너 개가 있다. 하나를 골라 Kafka UI → Topics → app-logs → Messages에서 그 줄의 본문(예: `ECU watchdog reset count=688`)으로 검색해 값(JSON)을 연다.

**보여야 하는 것.** 실측 예:

```
[consume #1194] p2@395 key=dev-0013 | event_time=2026-10-14T08:48:56.281347Z level=null file_offset=564 | INFO ECU watchdog reset count=688
```

`level`과 `category`가 null이고, `message`에는 원문 전체(`INFO ECU ...`)가 들어 있다. `event_time`은 원문에서 시각을 못 찾아 `collected_at`(Fluent Bit이 읽은 시각, 마이크로초까지 있음)으로 대체됐다. Kafka UI의 JSON에서 `event_time`과 `collected_at`이 같은지 본다.

**생각해 볼 것.** 이런 줄을 버리지 않고 `raw`를 그대로 남기는 이유는 무엇인가? 나중에 파서를 고치면 이 줄들을 어떻게 다시 처리할 수 있을까?

## D3. Fluent Bit 지표 읽기

**왜 하는가.** Fluent Bit이 몇 줄을 읽었고, 몇 줄을 보냈고, 몇 번 재시도했는지가 장애를 찾는 첫 단서다(README 5절).

**할 것.** 브라우저로 http://localhost:2020/api/v1/metrics 를 연다. 같은 내용을 터미널에서 필요한 부분만 본다.

```powershell
$m = Invoke-RestMethod http://localhost:2020/api/v1/metrics
$m.input.'tail.0' | Select-Object records, files_opened
$m.output.'http.0' | Select-Object proc_records, errors, retries, retries_failed
```

**보여야 하는 것.** `records`(읽은 줄)와 `proc_records`(보내서 2xx를 받은 줄)가 200으로 같고, `retries`가 0이다. `files_opened`는 파일 수(20)다.

## D4. 수집 서버가 죽어 있을 때

**왜 하는가.** 수집 서버(앱)가 내려간 동안 장비가 쓴 로그가 어떻게 되는지 본다. Fluent Bit은 2xx를 못 받으면 디스크 버퍼에 들고 있다가 다시 보낸다(`Retry_Limit False`, `storage.type filesystem`).

**할 것.**

1. 앱을 끈다.
2. 시뮬레이터는 앱 안에 있어서 못 쓰므로, 장비 파일에 직접 한 줄을 덧붙인다. `-Encoding ascii`를 빼먹지 않는다. 빼면 PowerShell 5.1이 UTF-16으로 써서 Fluent Bit이 깨진 글자를 읽는다.

```powershell
Add-Content -Path docker\devices\sim\dev-0001.log -Value "2026-10-15 15:00:00.000 [ERROR] manual line while app is down" -Encoding ascii
```

3. 10초 뒤 Fluent Bit 로그와 지표를 본다.

```powershell
docker logs --tail 6 fluent-bit
(Invoke-RestMethod http://localhost:2020/api/v1/metrics).output.'http.0' | Select-Object proc_records, errors, retries
```

4. 앱을 켠다. `partitions assigned`가 나온 뒤 앱 콘솔에서 `manual line while app is down`을 찾고, stats를 친다.

**보여야 하는 것.** 3에서 Fluent Bit 로그에 수집 서버로 접속하지 못했다는 오류(connection refused 같은 문구)와 `failed to flush chunk '...', retry in N seconds`가 보인다. `retries`가 늘어 있다. 4에서는 다음 재시도 때 그 줄이 들어와 콘솔에 consume 줄이 찍힌다. stats는 앱을 막 켰으므로 `receivedFromFluentBit 1`, `consumed 1`이다.

**안 될 때.** 앱을 켜고 1분이 넘도록 안 들어오면 Fluent Bit의 재시도 간격이 늘어난 것이다. 재시도할 때마다 대기 시간이 대략 두 배씩, 최대 2,000초까지 길어진다. 실측에서 `retry in 709 seconds`까지 본 적이 있다. 기다리기 싫으면 `docker restart fluent-bit`을 친다. 재시작한 Fluent Bit은 디스크 버퍼에 남은 묶음을 바로 다시 보낸다.

**생각해 볼 것.** 앱이 꺼져 있는 동안 그 한 줄은 어디에 있었는가? 장비(여기서는 fluent-bit 컨테이너)까지 재시작됐다면 어떻게 됐을까? (`storage.path`와 `DB`의 역할)

## D5. Kafka가 죽어 있을 때

**왜 하는가.** 이번에는 수집 서버는 살아 있고 그 뒤의 Kafka가 죽는다. 수집 서버는 Kafka ack를 받아야 200을 주므로(at-least-once 계약) 500을 돌려주고, Fluent Bit은 다시 보낸다. C8에서 같은 장애로 메시지 하나를 잃었던 것과 비교한다.

**할 것.**

1. 지금 숫자를 적어 둔다.

```powershell
Invoke-RestMethod http://localhost:8080/api/logs/stats
```

2. Kafka를 멈추고 50줄을 쓴다. 시뮬레이터는 파일만 쓰므로 Kafka가 없어도 바로 응답한다.

```powershell
docker stop kafka
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate?lines=50"
```

3. 75초쯤 기다리며 앱 콘솔을 보고, 그 뒤 Fluent Bit 로그를 본다.

```powershell
docker logs --tail 6 fluent-bit
```

4. Kafka를 다시 띄우고 1분 뒤 stats를 본다.

```powershell
docker start kafka
Start-Sleep 60
Invoke-RestMethod http://localhost:8080/api/logs/stats
```

**보여야 하는 것.** 2026-10-15 실측이다.

3에서 Fluent Bit이 보낸 요청들이 각각 60초 동안 붙잡혀 있다가 실패한다. C8과 같은 이유다(Kafka 4 클라이언트가 메타데이터를 잃고 `send()`가 `max.block.ms`만큼 막힌다). 앱 콘솔에 아래가 장비 파일 수만큼 찍힌다.

```
ERROR ... c.p.k.ingest.FluentBitIngestController : Kafka 발행 실패. Fluent Bit 에 재전송을 요청한다 (batch=5건)
```

Fluent Bit 로그에는 500 응답과 재시도 예약이 보인다.

```
[error] [output:http:http.0] host.docker.internal:8080, HTTP status=500
[ warn] [engine] failed to flush chunk '1-1792022392.227830294.flb', retry in 9 seconds: task_id=22, input=tail.0 > output=http.0 (out_id=0)
```

4에서는 Kafka를 띄우고 45초 안에 밀려 있던 줄이 모두 들어왔다. 앱은 따로 손대지 않아도 Kafka에 다시 붙었다. 실측 때는 앞선 실험에서 Fluent Bit 버퍼에 남아 있던 묶음까지 함께 들어와 증가분이 50보다 컸다. 00 파일의 깨끗한 상태에서 시작했다면 `receivedFromFluentBit`과 `consumed`가 1에서 적은 값보다 정확히 50씩 늘어야 한다. 그것이 유실 0이다.

**안 될 때.** 1분이 지나도 50이 다 안 찼으면 D4의 "안 될 때"처럼 재시도 간격이 늘어난 것이다. 조금 더 기다리거나 `docker restart fluent-bit`을 친다.

**생각해 볼 것.**

- C8에서는 같은 장애로 메시지 하나가 사라졌는데 여기서는 하나도 안 사라졌다. 차이를 만든 것은 무엇인가? (응답 코드의 의미, 다시 보내는 주체, 그 주체가 데이터를 들고 있는 곳)
- 같은 줄이 Kafka에 두 번 들어가는 시나리오를 하나 만들어 본다. 힌트: 수집 서버가 묶음 10건 중 7건까지 Kafka ack를 받은 상태에서 나머지가 실패해 500을 돌려주면, Fluent Bit은 무엇을 다시 보내는가? 그 중복은 `(device_id, source, offset)` 멱등키로 어디에서 걸러낼 것인가? (Step 4)

## D6. Fluent Bit 흉내 내기

**왜 하는가.** 수집 서버가 받는 요청이 정확히 어떤 모양인지 손으로 만들어 보낸다. 같은 요청을 두 번 보내 "재전송 = 중복"을 직접 만든다.

**할 것.** 아래를 통째로 복사해 붙여 넣는다. `@'`와 `'@`는 여러 줄 문자열의 시작과 끝이며, 닫는 `'@`는 반드시 줄 맨 앞에 있어야 한다. `source`는 실제 장비 파일과 겹치지 않도록 가짜 경로 `manual/dev-0900.log`를 쓴다.

```powershell
$body = @'
[
  {"collected_at":"2026-10-12T05:03:22.004Z","log":"2026-10-12 14:03:21.123 [INFO] ENGINE rpm=2400 temp=88C","source":"/var/log/devices/manual/dev-0900.log","offset":0},
  {"collected_at":"2026-10-12T05:03:22.004Z","log":"2026-10-12 14:03:21.456 [ERROR] CAN bus frame id=0x1A3 dropped","source":"/var/log/devices/manual/dev-0900.log","offset":58}
]
'@
Invoke-RestMethod -Method Post -Uri http://localhost:8080/ingest/fluentbit -ContentType 'application/json' -Body $body
```

응답 본문이 없으므로 터미널에는 아무것도 안 나온다(200). 마지막 `Invoke-RestMethod` 줄을 한 번 더 친다.

**보여야 하는 것.** 앱 콘솔에 `key=dev-0900` consume 줄이 두 번에 걸쳐 2줄씩, 모두 4줄 찍힌다. 두 번째의 `file_offset`도 0과 58로 첫 번째와 같다. Kafka UI Messages에서 `dev-0900`을 검색하면 4건이 있다. 지금 구조에는 중복을 거르는 곳이 없다. Step 4에서 ClickHouse의 ReplacingMergeTree가 같은 `(device_id, source, offset)`을 하나로 합친다.

## 다 됐으면

워크북 D절의 체크박스를 채운다. 00 파일 0-4에서 깨끗이 지운 상태로 시작해 B절과 D절의 "보여야 하는 것"이 그대로 나왔다면, Step 1·2를 "완료"로 올릴 근거가 된다(워크북 머리말). 결과를 알려 주면 진행 상황 표와 문서를 갱신한다.

생성기가 있으면 다음은 [05-OHT-생성기.md](05-OHT-생성기.md)다. 없으면 워크북 F절(스스로 설명하기)로 간다.
