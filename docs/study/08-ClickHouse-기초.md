# 08. ClickHouse 기초: Kafka 없이 손으로 넣어 보기

Step 4는 ClickHouse를 처음 다루므로 Kafka를 붙이기 전에 ClickHouse만 손으로 만져 본다. 이 장에서 익힐 것은 하나다. **INSERT 한 번이 디스크에 part 하나를 만들고, 백그라운드 병합이 part를 합친다.** 이것만 알면 다음 장에서 "왜 적재 컨슈머가 수천 행씩 묶어 넣는가"가 저절로 설명된다. 도구 소개는 `../concepts/03-주변-도구.md`의 ClickHouse 절에 있다. 손으로 따라 하는 절차는 `../practice/07-ClickHouse-적재.md` H1~H2다.

## 1. 띄우고 들어가기

ClickHouse는 Step 4부터 `docker/docker-compose.yml`에 들어 있다.

```yaml
clickhouse:
  image: clickhouse/clickhouse-server:25.8
  ports: ["8123:8123"]        # HTTP 인터페이스. 앱이 INSERT 하고, 브라우저 SQL 창(/play)도 여기
  environment:
    CLICKHOUSE_DB: logs
    CLICKHOUSE_USER: app
    CLICKHOUSE_PASSWORD: app
  volumes:
    - ./clickhouse/init:/docker-entrypoint-initdb.d:ro    # 처음 뜰 때 한 번 실행되는 SQL
```

SQL을 치는 방법은 둘이다. 브라우저에서 http://localhost:8123/play 를 열고 사용자 `app`, 암호 `app`을 넣는 것, 또는 컨테이너 안의 클라이언트를 쓰는 것이다.

```powershell
docker exec -it clickhouse clickhouse-client --user app --password app
```

## 2. 열 지향이라는 것

PostgreSQL이나 MySQL은 행 하나를 통째로 붙여 저장한다. ClickHouse는 컬럼 하나의 값들을 붙여 저장한다. `level` 컬럼만 모은 파일, `device_id` 컬럼만 모은 파일이 따로 있다고 생각하면 된다. 그래서 "장비별 ERROR 건수"처럼 수억 행에서 컬럼 두세 개만 읽는 집계는 그 컬럼 파일만 훑으면 되어 빠르다. 같은 컬럼의 값은 서로 비슷해서 압축도 잘 된다. `level`은 거의 INFO이고, 같은 장비의 `source`는 몇 가지 경로의 반복이다.

대신 행 하나를 고치거나 지우는 일은 느리다. 로그처럼 한 번 쓰면 바꾸지 않는 데이터에 맞는 이유다.

## 3. INSERT 한 번 = part 하나

연습 테이블을 만들고 세 번 나눠 넣는다.

```sql
CREATE TABLE logs.parts_demo (id UInt32, msg String) ENGINE = MergeTree ORDER BY id;
INSERT INTO logs.parts_demo VALUES (1, 'first insert');
INSERT INTO logs.parts_demo VALUES (2, 'second insert');
INSERT INTO logs.parts_demo VALUES (3, 'third insert');
SELECT name, rows, active FROM system.parts WHERE database = 'logs' AND table = 'parts_demo';
```

2026-10-19 실측 결과다.

```
all_1_1_0	1	1
all_2_2_0	1	1
all_3_3_0	1	1
```

행이 하나씩인 part 셋이 생겼다. part는 디스크의 폴더 하나이고, 그 안에 컬럼마다 파일이 있다. 이름 `all_2_2_0`은 "파티션 all, 블록 2번부터 2번까지, 병합 0회"라는 뜻이다. INSERT가 1행이든 10만 행이든 part 하나다. 그래서 1행씩 10만 번 넣으면 part 10만 개가 생기고, ClickHouse는 그것을 합치느라 바빠진다. 기본 설정에서는 한 파티션의 활성 part가 수천 개에 이르면 INSERT를 거부한다("Too many parts").

## 4. 병합이 part를 합친다

ClickHouse는 백그라운드에서 작은 part들을 큰 part로 합친다. 기다리면 언젠가 일어나지만, 실습에서는 바로 일으킨다.

```sql
OPTIMIZE TABLE logs.parts_demo FINAL;
SELECT name, rows, active FROM system.parts WHERE database = 'logs' AND table = 'parts_demo' AND active;
SYSTEM FLUSH LOGS;
SELECT event_type, count() FROM system.part_log WHERE database = 'logs' AND table = 'parts_demo' GROUP BY event_type;
```

```
all_1_3_1	3	1          ← 블록 1~3을 합친, 병합 1회짜리 part 하나
MergeParts	1
MergePartsStart	1
NewPart	3
```

`system.part_log`는 part가 생기고(`NewPart`) 합쳐진(`MergeParts`) 이력이다. compose가 `part_log.xml`로 켜 둔 기능이다. 적재 컨슈머가 돌 때 이 표를 보면 "INSERT가 몇 번 일어났고 병합이 따라가는가"를 알 수 있다.

## 5. 정렬 키와 파티션 키

`ORDER BY`는 part 안에서 행을 정렬하는 기준이다. 정렬된 순서로 저장되므로, 정렬 키 앞쪽 컬럼으로 거르는 쿼리(`WHERE device_id = 'OHT-0001'`)는 필요한 구간만 읽는다. 같은 값이 붙어 있으니 압축도 잘 된다. 일반 DB의 기본 키와 달리 유일성을 강제하지 않는다. 같은 정렬 키를 가진 행이 여러 개 있을 수 있다.

`PARTITION BY`는 part를 나누는 더 큰 단위다. 날짜로 나누면 하루치 데이터가 서로 다른 part에 섞이지 않는다. 보존 기간이 지난 날을 통째로 지우거나 하루치만 읽기 좋다. 너무 잘게 나누면(예: 장비별) INSERT 한 번이 여러 파티션에 걸쳐 part가 그만큼 늘어나므로, 로그는 보통 날짜(일 또는 월)로 나눈다.

이 프로젝트의 테이블이 무엇을 정렬 키와 파티션 키로 골랐는지는 다음 장에서 본다.

## 확인 질문

1. 같은 1만 행을 INSERT 한 번으로 넣을 때와 1행씩 1만 번 넣을 때, `system.parts`의 활성 part 수는 각각 어떻게 될까? 직접 넣어 보라(1만 번은 반복문 대신 1,000번으로 줄여도 된다).
2. `OPTIMIZE ... FINAL` 뒤 part 이름의 마지막 숫자가 1이 된 이유는?
3. 정렬 키가 유일성을 강제하지 않는다면, 같은 로그가 두 번 들어오는 것은 어떻게 막는가? (다음 장)
4. 파티션 키를 `device_id`로 하면 장비 1,000대의 로그를 한 번에 넣을 때 무슨 일이 생기는가?
