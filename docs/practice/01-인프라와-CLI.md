# 01. 인프라와 CLI (워크북 A절, Step 0)

Spring 앱 없이 Kafka만 띄워 메시지를 주고받는다. Kafka에 들어 있는 기본 명령줄 도구로 토픽을 만들고, 키를 붙여 보내고, 어느 파티션에 들어갔는지 본다. 앱 코드가 끼어들지 않으므로 Kafka 자체의 규칙만 보인다.

시작 전 상태: 00 파일의 0-4까지 끝나 컨테이너와 볼륨이 지워져 있다.

## A1. 인프라 기동

**왜 하는가.** `docker/docker-compose.yml` 하나가 컨테이너 다섯을 띄운다. kafka는 브로커, kafka-ui는 웹 화면, fluent-bit은 Step 2에서 쓸 수집 에이전트, clickhouse와 grafana는 Step 4(07 파일)에서 쓸 저장소와 대시보드다. kafka가 "healthy"가 되어야 나머지가 의미 있다.

**할 것.**

```powershell
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

`ps`는 30초쯤 뒤에 한 번 더 친다. 처음 실행이면 이미지를 내려받느라 몇 분 걸린다.

**보여야 하는 것.** `ps`의 STATUS 열에서 kafka와 clickhouse가 `Up ... (healthy)`, kafka-ui, fluent-bit, grafana가 `Up ...`. kafka가 `(health: starting)`이면 아직 기동 중이다. compose 파일의 healthcheck가 10초마다 브로커에 접속해 보고 성공하면 healthy로 바꾼다. kafka-ui는 kafka가 healthy가 된 뒤에야 뜬다(`depends_on: condition: service_healthy`). 2026-10-19에 빈 볼륨에서 `up -d`부터 다섯 개가 모두 뜨기까지 약 40초 걸렸다. grafana는 첫 기동 때 인터넷에서 플러그인을 받는다.

**안 될 때.** `port is already allocated`가 나오면 9092, 8085, 2020, 8123, 3000 중 하나를 다른 프로그램이 쓰고 있다. `Get-NetTCPConnection -LocalPort 9092 -State Listen`으로 어떤 프로세스인지 찾는다. kafka가 계속 `unhealthy`면 `docker logs kafka`의 마지막 30줄을 본다.

## A2. Kafka UI 둘러보기

**왜 하는가.** Kafka에는 GUI가 없다. Kafka UI는 브로커에 붙어 상태를 웹으로 보여 주는 별도 도구이며, 실습 내내 이 화면으로 확인한다. 메뉴 위치를 미리 익혀 둔다.

**할 것.** 브라우저에서 http://localhost:8085 를 연다. 왼쪽에 클러스터 `local-single`이 보인다. 그 아래 메뉴를 하나씩 눌러 본다.

| 메뉴 | 지금 보이는 것 | 앞으로 볼 것 |
|---|---|---|
| Brokers | 브로커 1개 (ID 1) | Step 6에서 3개가 된다 |
| Topics | 비어 있다. 내부 토픽 표시 스위치를 켜면 `__consumer_offsets`가 보일 수 있다 | `app-logs`의 Messages 탭에서 메시지 본문, 파티션, 오프셋 |
| Consumers | 비어 있다 | `log-printer` 그룹의 파티션별 커밋 오프셋과 lag |

**보여야 하는 것.** 위 표의 "지금 보이는 것". 토픽이 하나도 없어도 정상이다. `__consumer_offsets`는 컨슈머 그룹이 처음 생길 때 만들어지므로 볼륨을 지운 직후에는 없을 수 있다.

**안 될 때.** 페이지가 안 열리면 A1의 `ps`에서 kafka-ui가 떠 있는지 본다. 떠 있는데 클러스터가 빨간색(offline)이면 kafka가 아직 healthy가 아니다.

## A3. CLI로 토픽 만들고 주고받기

**왜 하는가.** "같은 키는 같은 파티션"이라는 규칙을 앱 코드 없이 확인한다. 이 규칙이 이 프로젝트에서 장비 단위 순서 보장의 근거다(키 = device_id).

**할 것.**

1. 명령 터미널에서 파티션 3개짜리 토픽 `hello`를 만들고 구성을 본다. `docker exec kafka <명령>`은 kafka 컨테이너 안에서 그 명령을 실행한다는 뜻이다. Kafka의 CLI 도구는 컨테이너 안 `/opt/kafka/bin`에 있다.

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic hello --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello
```

2. Terminal 탭 옆의 `+`로 **새 탭**(컨슈머 탭)을 열고 컨슈머를 띄운다. `-it`는 키보드 입력과 화면 출력을 컨테이너에 연결하는 옵션이다. 이 명령은 끝나지 않고 메시지를 기다린다.

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello --from-beginning --formatter-property print.key=true --formatter-property print.partition=true
```

3. 명령 터미널로 돌아와 프로듀서를 띄운다. `parse.key=true`와 `key.separator=:`는 입력한 줄을 `:` 앞은 키, 뒤는 값으로 나누라는 뜻이다. `>` 프롬프트가 뜨면 한 줄씩 입력하고 Enter를 누른다.

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello --reader-property parse.key=true --reader-property key.separator=:
```

```
>order:first
>order:second
>cart:third
>item:fourth
>order:fifth
```

4. 컨슈머 탭을 본다. 다 봤으면 두 탭 모두 Ctrl+C로 끝낸다.

**보여야 하는 것.** 컨슈머 탭에 `Partition:N  키  값` 형식으로 찍힌다. 키 이름은 일부러 골랐다. 파티션이 3개일 때 해시 결과로 `order`는 파티션 2, `cart`는 0, `item`은 1로 간다(2026-10-15, 10-19 확인). 계산식이 `murmur2(키) % 3`으로 고정이라 어느 PC에서 해도 같다.

```
Partition:2	order	first
Partition:2	order	second
Partition:0	cart	third
Partition:1	item	fourth
Partition:2	order	fifth
```

`order`의 세 메시지는 언제나 파티션 2에, 보낸 순서대로 들어간다. 컨슈머를 Ctrl+C로 끄고 2번 명령으로 다시 띄워 처음부터 읽게 하면 출력 순서가 바뀔 수 있다. 예를 들어 파티션 0의 것이 먼저 나온다. 컨슈머는 파티션별로 묶어서 가져오기 때문이다. Kafka가 보장하는 순서는 "한 파티션 안에서"뿐이다.

Kafka UI → Topics → hello → Messages 탭에서도 같은 메시지와 각자의 Partition, Offset을 볼 수 있다.

**안 될 때.** `--property`를 쓰는 옛 자료를 따라 쳐도 동작은 하지만 Kafka 4.2에서는 deprecated 경고가 나온다. 컨슈머용 이름은 `--formatter-property`, 프로듀서용 이름은 `--reader-property`다. 컨슈머 탭에 아무것도 안 나오면 두 명령의 토픽 이름이 같은지 본다.

**생각해 볼 것.** 키 없이 보내면 파티션이 어떻게 나뉘는가? 프로듀서를 `parse.key` 옵션 없이 다시 띄우고 몇 줄 보낸 뒤 컨슈머를 다시 띄워 확인한다. 힌트: 2026-10-15에 키 없는 메시지 4건을 보냈더니 4건 모두 한 파티션에 들어갔다. 돌아가며(round-robin) 나뉘지 않는 이유를 `../study/03-Producer-깊게.md` 4절의 "sticky"에서 찾는다.

## A4. 볼륨을 지우면 무엇이 사라지는가

**왜 하는가.** `down`과 `down -v`의 차이를 몸으로 익힌다. 실습이 꼬였을 때 되돌리는 방법이 이것이다.

**할 것.**

```powershell
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

`hello`가 남아 있는지 본다. kafka가 healthy가 되기 전에 `--list`를 치면 연결 오류가 날 수 있으니 `ps`로 확인하고 친다. 이어서 `-v`를 붙여 반복한다.

```powershell
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**보여야 하는 것.** `down` 후에는 `hello`가 그대로 있다. 컨테이너는 지워졌지만 데이터는 `kafka-data` 볼륨에 있었기 때문이다. `down -v` 후에는 목록이 비어 있다.

**생각해 볼 것.** 컨슈머 그룹이 "어디까지 읽었는가"(커밋 오프셋)는 어디에 저장되는가? `down`과 `down -v` 중 어느 쪽이 그것을 지우는가?

## A5. 명함 주소 바꿔 보기 (선택)

**왜 하는가.** `KAFKA_ADVERTISED_LISTENERS`는 브로커가 클라이언트에게 "앞으로는 이 주소로 오라"고 건네는 명함이다. 이 값이 클라이언트 위치에서 닿지 않는 주소면, 첫 접속(bootstrap)은 성공해도 그다음부터 실패한다. 입문자가 가장 많이 겪는 Kafka 접속 문제를 일부러 만들어 본다. 그림으로 먼저 보려면 `../concepts/01-kafka-listeners.html`을 연다.

이 항목은 앱을 띄워야 결과가 보인다. B1을 한 번 해 본 뒤에 돌아와서 해도 된다.

**할 것.**

1. `docker/docker-compose.yml`에서 아래 줄을 찾아 `localhost:9092`를 `kafka:9092`로 바꾸고 저장한다.

```yaml
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:19092,PLAINTEXT_HOST://kafka:9092"
```

2. `docker compose -f docker/docker-compose.yml up -d`를 친다. compose가 설정이 바뀐 kafka 컨테이너만 다시 만든다.
3. 앱을 켜고 앱 콘솔을 30초쯤 본다. Kafka UI도 새로 고쳐 본다.
4. 앱을 끄고, 1번에서 바꾼 것을 `localhost:9092`로 되돌린 뒤 `up -d`를 다시 친다.

**보여야 하는 것.** 앱 콘솔에 `kafka`라는 호스트를 찾지 못했다는 경고(`UnknownHostException: kafka`, `Error connecting to node kafka:9092` 같은 문구)가 반복되고, 정상 기동 때 나오는 `partitions assigned`가 끝내 안 나온다. 반면 Kafka UI는 계속 정상이다. 2026-10-19에 확인한 경고는 `Error connecting to node kafka:9092 (id: 1 rack: null isFenced: false)`와 `java.net.UnknownHostException: 알려진 호스트가 없습니다 (kafka)`였다. kafka 컨테이너의 healthcheck는 그동안에도 healthy였다. 컨테이너 안에서는 `kafka`라는 이름이 풀리기 때문이다.

**생각해 볼 것.** 같은 브로커인데 왜 Kafka UI는 되고 Spring 앱만 안 되는가? Kafka UI는 어느 리스너(포트)로 붙고, 그 리스너의 명함에는 어떤 주소가 적혀 있는가? (`../study/01-환경과-프로젝트-구조.md` 4절)

## 다 됐으면

워크북 A절의 체크박스를 채운다. 실습용 `hello` 토픽은 A4의 `down -v`로 이미 지워졌다. 다음은 [02-앱으로-발행과-소비.md](02-앱으로-발행과-소비.md)다. 컨테이너는 띄운 채로 둔다.
