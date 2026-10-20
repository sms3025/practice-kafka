# Step 3. 파티션과 컨슈머 그룹 결과

## 만든 것

`app-logs`를 파티션 6개로 늘리고, 리스너마다 컨슈머 스레드 3개를 띄우고, 두 번째 컨슈머 그룹을 붙였다. 앱을 포트만 바꿔 두 개 띄워 리밸런스를 관찰한다.

```
                    app-logs (파티션 6, key=device_id)
                 ┌──────────────┬──────────────┐
 log-printer 그룹 │ 앱 A 스레드 3 │ 앱 B 스레드 3 │   ← 같은 그룹: 파티션을 나눠 읽는다
 log-stats 그룹   │ 앱 A 스레드 3 │ 앱 B 스레드 3 │   ← 다른 그룹: 같은 메시지를 따로 전부 읽는다
                 └──────────────┴──────────────┘
```

| 파일 | 역할 |
|---|---|
| `application.properties` | `app.kafka.topics.app-logs.partitions=6`, `spring.kafka.listener.concurrency=3`, `app.kafka.consumer-groups.stats=log-stats`, Actuator `shutdown` 공개 |
| `consumer/LogStatsConsumer.java` | 두 번째 그룹. 레벨별 건수만 센다 |
| `api/LogController.java` | `/api/logs/stats`에 레벨별 건수, `GET /api/logs/assignments`(스레드별 할당) |
| `LogPipelineIntegrationTest` | 두 그룹이 같은 메시지를 받는지, 기존 토픽이 6개로 늘어나는지 |

## 설계에서 눈여겨볼 것

**파티션 수는 병렬의 상한이자 순서의 단위다.** 6개를 고른 것은 앱 1개(스레드 3)일 때 스레드당 2개, 앱 2개(6스레드)일 때 1개씩으로 나뉘는 모습과, 앱을 더 띄우면 남는 스레드가 노는 모습을 모두 보기 위해서다. 처리량 근거는 `kafka-load-test` 브랜치의 측정(적재 컨슈머 1개가 장비 약 2,400대 분량)을 인용했다. 6개면 1,000대의 몇 배까지 컨슈머를 늘릴 여지가 있다.

**파티션을 늘리면 키의 자리가 바뀐다.** `KafkaAdmin`이 기동 때 3개를 6개로 늘렸다. `murmur2(key) % 파티션 수`의 분모가 바뀌므로, 늘린 시점 앞뒤로 같은 장비의 로그가 다른 파티션에 있을 수 있다. 옛 메시지를 다 소비하기 전까지는 장비 하나의 순서가 보장되지 않는다. 줄이는 것은 Kafka가 허용하지 않는다.

**두 번째 그룹은 하는 일이 다른 소비자를 서로 영향 없이 붙이는 방법이다.** `log-stats`는 `log-printer`와 오프셋을 따로 가지므로, 한쪽이 느려져도 다른 쪽의 lag은 그대로다. Step 4의 적재 컨슈머도 같은 방식(세 번째 그룹 `clickhouse-sink`)으로 붙는다.

**정상 종료 수단을 열어 두었다.** Windows에서는 `Stop-Process`가 강제 종료라 정상 종료와의 차이를 볼 수 없다. Actuator `shutdown`을 실습용으로 열고, 운영에서는 열지 말라고 주석에 적었다.

## 실행 절차

```powershell
# 0. 인프라와 jar
docker compose -f docker/docker-compose.yml up -d
$env:JAVA_HOME = "C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr"; .\gradlew.bat bootJar
$java = "C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr\bin\java.exe"

# 1. 앱 A (터미널 1). 콘솔에 "increasing" 과 partitions assigned 가 찍힌다
& $java -jar build\libs\practice-kafka-0.0.1-SNAPSHOT.jar

# 2. (명령 터미널) 할당과 두 그룹의 건수
Invoke-RestMethod http://localhost:8080/api/logs/assignments | ConvertTo-Json -Depth 4
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/generate?count=1000&devices=50"
Invoke-RestMethod http://localhost:8080/api/logs/stats

# 3. 앱 B (터미널 2). A 콘솔에 revoked → assigned
& $java -jar build\libs\practice-kafka-0.0.1-SNAPSHOT.jar --server.port=8081

# 4. 정상 종료와 강제 종료
Invoke-RestMethod -Method Post http://localhost:8081/actuator/shutdown -ContentType application/json
Stop-Process -Id (Get-NetTCPConnection -LocalPort 8081 -State Listen).OwningProcess -Force   # B 를 다시 띄운 뒤
```

명령마다의 기대 화면은 `../practice/06-파티션과-컨슈머그룹.md`에 있다.

## 확인해야 할 것

2026-10-19 이 PC에서 실측한 값이다.

- 기동 로그: `Topic 'app-logs' exists but has a different partition count: 3 not 6, increasing if the broker supports it`. 그 뒤 `kafka-topics --describe`의 `PartitionCount: 6`.
- 앱 A 혼자: `log-printer`의 스레드 셋이 `[0,1]`, `[2,3]`, `[4,5]`. `log-stats`도 같다.
- 1,000건 발행: `consumed` +1,000, `statsTotal` +1,000. 처음 뜬 `log-stats`는 과거분 121,403건까지 읽었다.
- 앱 B 합류: A의 스레드 셋이 파티션을 모두 내려놓고(44.177) A가 셋, B가 셋을 받음(44.190~44.217). 1,000건 발행 시 `log-printer`는 A 436건, B 564건, `log-stats`는 A 492건, B 508건.
- B 정상 종료: 약 1.4초 뒤 A가 6개를 모두 받음.
- B 강제 종료: 약 44초 뒤 A가 6개를 받음(`session.timeout.ms` 45초).
- B를 `--spring.kafka.listener.concurrency=6`으로: 그룹 컨슈머 9개 중 3개가 빈 할당 `[]`.

같은 날 오후, 볼륨을 모두 지운 깨끗한 환경에서 실습 00부터 다시 따라 와 G1~G7을 재현했다(`../practice/README.md` 7절). 결론은 모두 같았고 숫자만 조금 달랐다. 정상 종료는 0.2초, 강제 종료는 45.6초 뒤 A가 6개를 받았다. B 합류 때 1,000건은 A 578건, B 422건으로 나뉘었다. 새 프로토콜(G7)에서도 1, 4, 5번만 넘어갔다. 다르게 나온 것은 둘이다. `log-stats`는 B절부터 앱과 함께 떠 커밋해 왔으므로 G3에서 읽을 과거분이 0이었다. 그리고 앱이 뜬 직후 `log-stats`의 한 스레드가 3초 동안 빈 할당이었다가 리밸런스 한 번 뒤 둘씩으로 나뉘었다. 늦게 합류한 스레드 때문이며, compose가 `group.initial.rebalance.delay.ms`를 0으로 둔 결과다. 실습은 G절 동안 `--app.sink.enabled=false`로 적재 컨슈머를 끄고 한다(그 그룹의 리밸런스까지 섞이지 않게).

## 실험 (해볼 것)

- **새 그룹 프로토콜.** 두 앱을 `"--spring.kafka.consumer.properties[group.protocol]=consumer"`로 띄우면, B가 합류할 때 A가 6개 중 3개만 내놓고 나머지는 멈추지 않는다. 옮겨진 파티션은 5~7초 쉰다(2026-10-19 실측, `study/07` 5절 심화).
- **세션 타임아웃.** `--spring.kafka.consumer.properties[session.timeout.ms]=10000`으로 띄우고 강제 종료하면 넘겨받기까지의 시간이 약 10초로 줄어야 한다(아직 실측하지 않았다).

## 테스트

`./gradlew test` 12건 통과(2026-10-19). Step 3이 더한 것은 둘이다. `LogPipelineIntegrationTest.producedEventIsConsumedByBothGroups`는 메시지 하나를 `log-printer`와 `log-stats`가 모두 받는지, `existingTopicIsGrownToConfiguredPartitions`는 임베디드 브로커에 3개로 만든 토픽이 앱 기동 뒤 6개가 되는지 고정한다.

## 배운 점

리밸런스의 비용은 "얼마나 자주"와 "누가 어떻게 떠나는가"가 정한다. 정상 종료는 1.4초, 강제 종료는 44초였다. 같은 일이 일어나도 컨슈머가 떠난다는 말을 하고 가느냐에 따라 30배 차이가 난다. 운영에서 배포 스크립트가 컨슈머를 정상 종료시키는지, 컨테이너 종료 유예 시간이 충분한지가 숫자 하나로 설명된다.

파티션을 늘리는 일은 설정 한 줄이지만 키의 자리를 바꾼다. 파티션 수를 처음에 넉넉히 정하라는 조언이 "나중에 늘리기 어려워서"가 아니라 "늘릴 때 순서가 흔들려서"라는 것을 로그와 오프셋으로 확인했다.
