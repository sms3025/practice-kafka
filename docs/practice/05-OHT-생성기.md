# 05. OHT 로그 생성기 (워크북 E절, `study/05`)

기업이 제공한 생성기로 실제 형식의 OHT 차량 로그를 만들어 파이프라인에 흘린다. 연습용 시뮬레이터와 달리 차량마다 폴더가 있고, 형식이 다른 로그 두 계열이 섞이며, 양이 많다. 생성기가 무엇을 만드는지와 파이프라인이 그것을 어떻게 다루는지는 `../study/05-OHT-로그-연동.md`에 있다. 여기서는 손으로 돌리는 순서만 다룬다.

**대외비.** 생성기(`tools/ohtsim.pyz`), 그 사용법 문서, 생성된 로그는 대외비이고 이 저장소는 공개되어 있다. 생성 로그를 캡처하거나 복사해 문서, 이슈, 커밋 메시지, 메신저에 붙이지 않는다. 워크북에는 숫자(건수, 초)만 적는다.

시작 전 상태: 04 파일을 마쳐 컨테이너 셋과 앱이 떠 있다. 00 파일 0-5의 Python 설치를 마쳤다.

## E1. 준비

**왜 하는가.** 생성기와 생성 로그가 git에 잡히지 않는지 먼저 확인한다. 실수로 커밋하면 공개 저장소에 올라간다.

**할 것.**

```powershell
python --version
Test-Path tools\ohtsim.pyz
git check-ignore -v tools/ohtsim.pyz docker/devices/OHT-0001/x.log
git status --short
```

**보여야 하는 것.** `Python 3.x.y`, `True`. `git check-ignore`는 두 경로 각각에 대해 `.gitignore`의 어느 줄이 막는지 보여 준다(`/tools/`, `/docker/devices/*` 같은 규칙). `git status --short` 목록에 `tools/`나 `docker/devices/`로 시작하는 줄이 없다.

**안 될 때.** `Test-Path`가 `False`면 생성기 파일을 프로젝트의 `tools` 폴더에 복사한다(사용법 문서는 저장소 밖에 둔다). `git check-ignore`가 아무것도 출력하지 않으면 그 경로가 무시되지 않는다는 뜻이다. 진행하지 말고 `.gitignore`를 확인한다.

## E2. 실시간 3대

**왜 하는가.** 차량 3대가 실제 속도로 로그를 쓰는 상황을 만든다. 차량 1대가 초당 약 60줄을 쓰므로 3대면 초당 180줄쯤이다.

**할 것.**

1. 콘솔이 consume 줄로 덮이지 않게 `application.properties`에 `logging.level.com.practice.kafka.consumer=WARN`을 넣고 앱을 재시작한다(03 파일 C0와 같은 줄이다). 대신 숫자는 stats로 센다.
2. Terminal 탭을 하나 더 열고(생성기 탭) 생성기를 띄운다. PowerShell에서는 현재 폴더의 스크립트를 실행할 때 앞에 `.\`를 붙여야 한다.

```powershell
.\scripts\ohtsim.cmd --vehicles 3 --seed 1
```

3. 명령 터미널에서 5초마다 네 숫자를 찍는 감시 루프를 띄운다. 멈출 때는 Ctrl+C를 누른다. 이 루프는 E4에서도 쓴다.

```powershell
while ($true) {
    $fb = Invoke-RestMethod http://localhost:2020/api/v1/metrics
    $st = Invoke-RestMethod http://localhost:8080/api/logs/stats
    "{0}  fb_in={1}  fb_out={2}  received={3}  consumed={4}" -f (Get-Date -Format HH:mm:ss), $fb.input.'tail.0'.records, $fb.output.'http.0'.proc_records, $st.receivedFromFluentBit, $st.consumed
    Start-Sleep 5
}
```

4. 1분쯤 지켜본 뒤 생성기 탭에서 Ctrl+C로 생성기를 멈춘다. 감시 루프에서 숫자가 더 이상 안 늘 때까지 기다린 뒤 루프도 멈춘다.

**보여야 하는 것.** 네 숫자가 5초마다 수백씩 함께 늘고, 서로 몇 초 차이로 따라간다. 생성기를 멈추고 잠시 뒤에는 넷이 같은 값에서 멈춘다. `fb_in`과 `fb_out`은 Fluent Bit이 띄워진 뒤의 누적이라 앞 실습의 건수가 포함돼 있다. `received`와 `consumed`는 앱 재시작 뒤부터 센 값이다. 1분 뒤 `received` 값을 워크북 E2 빈칸에 적고, 생성기가 끝날 때 보여 주는 누적 줄 수와 비교한다.

`docker\devices` 아래에 `OHT-0001`~`OHT-0003` 폴더가 생기고, 그 안에 로거 계열별 하위 폴더와 시간 단위 파일이 있다(구조는 `study/05` 1절).

**안 될 때.** `python`을 찾을 수 없다고 나오면 00 파일 0-5로 돌아간다. 숫자가 `fb_in`에서만 늘고 `fb_out`이 멈춰 있으면 앱이 꺼져 있거나 Kafka가 멈춰 있다(04 파일 D4, D5).

## E3. 두 형식이 하나의 스키마로

**왜 하는가.** 형식이 다른 두 로거 계열의 줄이 같은 `LogEvent` 스키마로 정규화되는 것을 본다. 시각 표기, 소수 자리, 레벨 표기가 서로 달라도 결과는 한 모양이어야 한다.

**할 것.** Kafka UI → Topics → app-logs → Messages에서 `SEQ_INFO`로 검색해 하나를 열고, `APP`으로 검색해 하나를 연다. 두 메시지의 값을 나란히 놓고 아래 네 필드를 비교한다. 화면을 캡처해 밖으로 보내지 않는다.

**보여야 하는 것.**

- `device_id`가 파일 이름이 아니라 폴더 이름(`OHT-000N`)에서 왔다.
- `category`에 계열명(`SEQ_INFO`)이나 로거명(`APP`)이 들어 있다.
- `raw`의 시각은 소수 자리 수와 구분자(`.`와 `,`)가 서로 다른데, `event_time`은 둘 다 `Z`로 끝나는 UTC다. 한국 시각보다 9시간 이르다.
- `raw` 쪽에서 `INFORMATION`으로 적힌 레벨이 `level`에서는 `INFO`로 통일돼 있다.

## E4. 한꺼번에 몰아넣기

**왜 하는가.** 실시간이 아니라 5분 몫을 기다리지 않고 한꺼번에 만든다. 장비들이 오래 끊겼다가 복구되어 밀린 로그를 한 번에 쏟아내는 상황과 같다. 파이프라인의 어디가 먼저 밀리는지 본다.

**할 것.**

1. 앱을 끄고 깨끗한 상태로 되돌린다(README 6절). 과거 시각을 지정해 만들 때는 빈 폴더에 써야 하기 때문이다.

```powershell
docker compose -f docker/docker-compose.yml down -v
Remove-Item -Recurse -Force docker\devices\sim, docker\devices\OHT-* -ErrorAction SilentlyContinue
docker compose -f docker/docker-compose.yml up -d
```

2. kafka가 healthy가 되면 앱을 켠다(E2에서 넣은 consumer WARN 설정은 그대로 둔다). 명령 터미널에서 E2의 감시 루프를 다시 띄운다.
3. 생성기 탭에서 10대 5분 몫을 최대 속도로 만든다.

```powershell
.\scripts\ohtsim.cmd --vehicles 10 --seed 1 --speed 0 --duration 5m --start 2026-01-05T09:00:00
```

4. 감시 루프와 Kafka UI → Consumers → log-printer의 lag을 번갈아 본다. lag이 치솟았다가 0으로 돌아올 때까지 걸린 시간을 잰다.

**보여야 하는 것.** 생성기는 몇 초 만에 끝난다(10대 × 5분 × 초당 60줄, 약 18만 줄). 그 뒤로 감시 루프의 네 숫자가 차례로 따라온다. 어느 두 숫자 사이가 가장 오래 벌어져 있는지 본다. 그 구간이 병목이다. lag이 0으로 돌아오는 데 걸린 시간을 워크북 E4 빈칸에 적는다. 이 측정은 아직 이 저장소에서 해 보지 않았으므로 기준 수치가 없다. 처음 잰 값이 기준이 된다.

**생각해 볼 것.** 병목은 어디였는가? Fluent Bit 읽기(`fb_in`이 천천히 오름), HTTP 전송(`fb_in`과 `fb_out` 사이가 벌어짐), Kafka 발행(`fb_out`은 올랐는데 `received`가 늦음 — 이 둘은 거의 붙어 다녀야 정상이다), 컨슈머(`received`와 `consumed` 사이, Kafka UI lag) 중 하나를 고르고 근거를 숫자로 댄다. 여유가 있으면 consumer 로그를 INFO로 되돌려 같은 실험을 한 번 더 하고 lag이 빠지는 시간을 비교한다. 콘솔 출력 하나가 컨슈머 처리량을 얼마나 깎는지 보인다(`study/05` 4절이 Step 4의 배치 리스너로 이어지는 이유).

## E5. 정리

**왜 하는가.** 생성 로그는 대외비이고 용량도 크다. 실습이 끝나면 지운다.

**할 것.**

1. 생성기와 감시 루프를 Ctrl+C로 멈추고 앱을 끈다.
2. E2에서 넣은 `logging.level.com.practice.kafka.consumer=WARN` 줄을 지운다.
3. README 6절대로 지우고 다시 띄운다(다음 실습을 이어서 한다면). 그리고 git에 아무것도 잡히지 않았는지 본다.

```powershell
docker compose -f docker/docker-compose.yml down -v
Remove-Item -Recurse -Force docker\devices\sim, docker\devices\OHT-* -ErrorAction SilentlyContinue
git status --short
```

**보여야 하는 것.** `docker\devices`에는 `.gitkeep`만 남는다. `git status --short`에 `tools/`, `docker/devices/`, `*.pyz`가 보이지 않는다.

## 다 됐으면

워크북 E절의 빈칸을 채운다. 마지막으로 워크북 F절(스스로 설명하기)의 일곱 질문에 각각 다섯 문장 이내로 답해 본다. 막히는 질문이 있으면 그 질문의 힌트에 나온 실습으로 돌아간다. 다 답할 수 있으면 Step 3으로 간다.
