# 실습 안내: 처음부터 그대로 따라 하는 손 연습

`docs/study/06-실습-워크북.md`는 체크리스트다. 무엇을 할지와 기대 결과를 한 줄씩 적어 두었기 때문에 이미 아는 사람에게는 빠르지만, 처음 하는 사람은 "어느 터미널에서, 어떤 명령을, 무엇을 보고 넘어가는가"에서 막힌다. 이 폴더는 워크북의 각 항목을 그 수준까지 풀어 쓴 것이다. 항목 번호(A1, B2, ...)는 워크북과 같으므로, 여기서 따라 하고 결과(측정값, 체크)는 워크북에 적는다.

## 1. 파일과 순서

| 순서 | 파일 | 워크북 절 | 무엇을 하는가 | 걸리는 시간 |
|---|---|---|---|---|
| 0 | [00-준비.md](00-준비.md) | | 도구 확인, 터미널 배치, 깨끗한 상태 만들기 | 20분 |
| 1 | [01-인프라와-CLI.md](01-인프라와-CLI.md) | A | docker로 Kafka를 띄우고 CLI로 메시지를 주고받는다 | 40분 |
| 2 | [02-앱으로-발행과-소비.md](02-앱으로-발행과-소비.md) | B | Spring 앱으로 발행·소비하고 오프셋과 컨슈머 그룹을 본다 | 50분 |
| 3 | [03-프로듀서-설정-실험.md](03-프로듀서-설정-실험.md) | C | 설정을 바꿔 가며 처리량과 배치 크기를 잰다. Kafka가 죽었을 때를 본다 | 1.5시간 |
| 4 | [04-fluent-bit-수집경로.md](04-fluent-bit-수집경로.md) | D | 파일 → Fluent Bit → 수집 서버 → Kafka 경로를 돌리고 장애를 넣어 유실 0을 확인한다 | 1시간 |
| 5 | [05-OHT-생성기.md](05-OHT-생성기.md) | E | 대외비 생성기로 실제 형식의 로그를 흘린다 (생성기가 있을 때만) | 40분 |

순서대로 한다. 뒤 파일은 앞 파일에서 띄운 환경과 익힌 명령을 그대로 쓴다. 각 파일 끝에서 워크북 해당 절의 체크박스를 채우고, 모두 끝나면 워크북 F절(스스로 설명하기)로 간다.

## 2. 항목 하나의 모양

모든 항목은 같은 다섯 칸으로 쓴다.

- **왜 하는가**: 이 항목으로 무엇을 확인하려는지 한두 문장.
- **할 것**: 번호 붙은 단계. 명령은 복사해 그대로 붙여 넣으면 된다.
- **보여야 하는 것**: 정상일 때 콘솔, Kafka UI, 응답에 나오는 것.
- **안 될 때**: 자주 걸리는 지점. 여기에 없으면 `../study/01-환경과-프로젝트-구조.md` 7절 표를 본다.
- **생각해 볼 것**: 워크북의 질문. 답은 실행 결과를 보고 스스로 적는다.

## 3. 명령 표기 규칙

이 실습은 Windows, Docker Desktop, IntelliJ를 기준으로 쓴다. 명령은 **PowerShell**용이다. IntelliJ 아래쪽 Terminal 탭을 열면 기본으로 PowerShell이 뜨고, 위치는 프로젝트 루트다. 모든 명령은 프로젝트 루트에서 실행한다고 가정한다.

HTTP 요청은 PowerShell 내장 명령 `Invoke-RestMethod`로 보낸다. 응답 JSON을 표나 객체로 바로 보여 주고, PowerShell 5.1과 7 모두에서 같은 문법으로 돈다.

```powershell
Invoke-RestMethod http://localhost:8080/api/logs/stats                                   # GET
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/generate?count=1000"      # POST, 본문 없음
```

인터넷 자료의 `curl -X POST ...`를 그대로 치지 않는다. Windows PowerShell 5.1에서 `curl`은 `Invoke-WebRequest`의 별칭이라 `-X`를 모른다는 오류가 난다. curl을 쓰고 싶으면 `curl.exe`라고 확장자까지 적는다. JSON 본문을 보내는 요청은 PowerShell 따옴표 규칙 때문에 curl.exe보다 `Invoke-RestMethod -Body '...'`가 덜 헷갈린다.

IntelliJ의 HTTP Client를 쓸 수 있으면 `http/step1.http`, `http/step2.http`를 열어 요청 옆 실행 버튼을 눌러도 된다. 같은 요청이 들어 있다.

Git Bash는 쓰지 않는다. Git Bash에서 `docker exec kafka /opt/kafka/bin/...`을 치면 `/opt/...`가 `C:/Program Files/Git/opt/...`로 바뀌어 "no such file" 오류가 난다.

## 4. 화면 배치

터미널 탭 둘과 브라우저 탭 셋을 열어 두고 시작한다. 실습 내내 이 배치를 유지한다.

| 이름 | 무엇 | 용도 |
|---|---|---|
| 앱 콘솔 | IntelliJ Run 창 (또는 `bootRun`을 띄운 터미널 탭) | 발행·소비 로그를 본다. 앱을 끄고 켜는 곳 |
| 명령 터미널 | IntelliJ Terminal 탭 | docker 명령과 HTTP 요청을 친다 |
| Kafka UI | http://localhost:8085 | 토픽, 메시지, 컨슈머 그룹, lag을 본다 |
| Fluent Bit 지표 | http://localhost:2020/api/v1/metrics | 읽은 줄 수, 보낸 줄 수, 재시도 수 (Step 2부터) |
| 앱 통계 | http://localhost:8080/api/logs/stats | 수집 서버가 받은 건수와 컨슈머가 소비한 건수 |

CLI로 메시지를 주고받는 A3만 터미널 탭이 하나 더 필요하다.

## 5. 관찰 도구 넷과 읽는 법

파이프라인 어딘가가 막히면 아래 네 숫자를 앞에서부터 비교한다. 어느 두 숫자 사이가 벌어졌는지가 막힌 구간이다.

```
Fluent Bit input.tail.0.records ─▶ output.http.0.proc_records ─▶ /api/logs/stats receivedFromFluentBit ─▶ consumed
      (파일에서 읽은 줄)               (수집 서버가 2xx로 받아 준 줄)     (수집 서버가 Kafka ack까지 받은 줄)        (컨슈머가 처리한 줄)
```

input은 늘었는데 proc_records가 안 늘면 수집 서버로 못 보내는 중이다. received와 consumed가 벌어지면 컨슈머가 늦거나 멈춘 것이고, 이때 Kafka UI의 Consumers 화면에서 lag(아직 안 읽은 메시지 수)이 커진다. `/api/logs/stats`의 두 숫자는 앱 메모리에 있는 카운터라 **앱을 재시작하면 0부터 다시 센다**.

Step 1 실험(C절)에서는 다섯 번째 도구로 앱의 Actuator 지표를 쓴다. 프로듀서가 요청을 몇 번 보냈는지, 배치가 평균 몇 바이트였는지가 나온다. 사용법은 03 파일에 있다.

## 6. 깨끗한 상태로 되돌리기

실험이 꼬였거나 처음부터 다시 하고 싶으면 아래를 순서대로 친다. Kafka의 토픽과 오프셋, Fluent Bit의 "어디까지 읽었는가" 기록과 디스크 버퍼, 연습용 로그 파일이 모두 지워진다.

```powershell
# 1) 앱을 먼저 끈다 (IntelliJ Run 창의 빨간 정지 버튼, 또는 bootRun 터미널에서 Ctrl+C)
# 2) 컨테이너와 볼륨 삭제
docker compose -f docker/docker-compose.yml down -v
# 3) 연습용 로그와 생성기 로그 삭제 (.gitkeep 은 남긴다)
Remove-Item -Recurse -Force docker\devices\sim, docker\devices\OHT-* -ErrorAction SilentlyContinue
# 4) 다시 띄운다
docker compose -f docker/docker-compose.yml up -d
```

볼륨만 지우고 로그 파일을 남기거나, 로그 파일만 지우고 볼륨을 남기면 Fluent Bit이 옛 파일을 처음부터 다시 보내거나 새 파일을 건너뛴다. 둘은 항상 같이 지운다.

## 7. 이 안내의 검증 상태

이 폴더의 명령은 2026-10-14~15에 이 저장소의 Windows PC에서 확인했다. 그중 실제로 돌려서 출력을 본 항목은 A3(입력을 파이프로 넣어서), B1, B2, B3, C0의 측정 스크립트, C7의 컨슈머 쪽, C8, D1, D2, D3, D5, E1의 git 확인이다. 이 항목들의 "보여야 하는 것"에 실측 출력과 수치를 적었다. 나머지 항목의 기대 결과는 코드와 설정에서 이끌어 낸 것이다. 직접 해 보고 다르게 나오면 그 자리에서 문서를 고치고, 오류와 해결은 `../study/01-환경과-프로젝트-구조.md` 7절 표에 한 줄 보탠다.

실측 중에 문서나 의도와 다른 동작을 넷 발견해 2026-10-15에 고쳤다.

1. Windows에서 `POST /api/logs`가 500으로 실패했다. `LogNormalizer`가 `manual://dev-0001.log`를 `Paths.get`에 넘겨 `InvalidPathException`이 났다. 문자열로 파일 이름을 자르도록 고치고 테스트를 하나 더했다.
2. 문서에는 Kafka가 멈춰도 `send()`가 즉시 반환하고 2분 뒤 실패한다고 적혀 있었다. 실제로는 60초 동안 막혔다가 예외를 던졌다(Kafka 4의 rebootstrap). 03 파일 C8, 04 파일 D5, `study/03`에 실제 동작대로 적었다.
3. 2의 예외가 `FluentBitIngestController`의 try 바깥에서 터져, 설계에 적힌 `Kafka 발행 실패` 로그 없이 Spring 기본 오류 처리로 500이 나갔다. `send()` 루프를 try 안으로 옮겼다.
4. 키 없이 보낸 메시지를 컨슈머가 처리하지 못하고 10번 재시도 뒤 버렸다(키 헤더를 필수로 받고 있었다). `required = false`로 바꿨다. 워크북 C7이 이 경우다.
