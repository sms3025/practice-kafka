package com.practice.kafka.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import com.practice.kafka.log.LogEvent;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 로그 묶음을 ClickHouse 에 INSERT 한 번으로 넣는다. 형식은 RowBinaryWithDefaults 다 (Step 4).
 *
 * <p>RowBinary 는 ClickHouse 의 바이너리 행 형식이다. 문자열은 길이 + 바이트, 시각은 정수로 보내므로 서버가 JSON 처럼 글자를 읽어
 * 해석할 필요가 없다. 대신 그 변환을 이 쪽(컨슈머)이 한다. "WithDefaults" 는 값을 비운 컬럼에 테이블의 DEFAULT 를 쓰라는 표시를
 * 보낼 수 있는 변형이다. {@code inserted_at} 을 비워 두어 ClickHouse 가 기록하는 순간의 시각({@code now64(3)})이 들어가게 한다.
 *
 * <p>INSERT 한 번은 ClickHouse 디스크에 part 하나를 만든다(묶음이 파티션 하나에 들 때). 그래서 묶음이 작고 잦으면 part 가 쌓여 병합이 밀린다.
 * 적재 컨슈머(Step 4)는 poll 한 번의 묶음을, 직접 적재 경로(Step 4+)는 HTTP 요청 하나의 묶음을 이것으로 넣는다.
 * 어느 테이블에 어떤 설정(예: {@code async_insert})으로 넣을지는 만드는 쪽이 정한다.
 */
public class ClickHouseWriter {

    private final Client client;
    private final String table;
    private final Supplier<InsertSettings> settings;
    private volatile TableSchema schema;

    /**
     * @param table    넣을 테이블 (기본 DB 기준 이름)
     * @param settings INSERT 마다 새로 만들 설정. 보통 INSERT 는 {@code InsertSettings::new},
     *                 비동기 INSERT 는 {@code async_insert=1} 을 붙인 것
     */
    public ClickHouseWriter(Client client, String table, Supplier<InsertSettings> settings) {
        this.client = client;
        this.table = table;
        this.settings = settings;
    }

    public String table() {
        return table;
    }

    /** events 를 INSERT 한 번으로 넣는다. 실패하면 예외를 던진다. 다시 시도할지는 부른 쪽(리스너의 에러 핸들러, Fluent Bit)이 정한다. */
    public void insert(List<LogEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        try (InsertResponse ignored = client.insert(table, out -> write(out, events),
                ClickHouseFormat.RowBinaryWithDefaults, settings.get()).get(120, TimeUnit.SECONDS)) {
            // 응답이 왔다 = ClickHouse 가 기록했다 (비동기 INSERT 도 wait_for_async_insert=1 이면 기록 뒤에 응답한다)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("ClickHouse INSERT 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new IllegalStateException("ClickHouse INSERT 실패 (" + table + ", " + events.size() + "건)", e);
        }
    }

    /**
     * 컬럼 순서와 타입. RowBinary 는 컬럼 이름 없이 순서대로 값을 보내므로 서버에서 읽어 둔다.
     * 앱이 뜰 때가 아니라 첫 INSERT 때 읽는다. 그래야 ClickHouse 가 아직 안 떠 있어도 앱(수집 서버)은 뜨고,
     * 적재만 ClickHouse 가 뜰 때까지 재시도한다. 수집과 적재가 서로의 장애에 묶이지 않게 하는 것이 Kafka 를 사이에 둔 이유다.
     */
    private TableSchema schema() {
        TableSchema s = schema;
        if (s == null) {
            s = client.getTableSchema(table);
            schema = s;
        }
        return s;
    }

    /**
     * 테이블 컬럼 이름 → LogEvent 에서 값을 꺼내는 방법. 테이블(docker/clickhouse/init/01-schema.sql)과 이름이 같아야 하며,
     * {@code ClickHouseWriterColumnsTest} 가 둘이 어긋나지 않는지 확인한다. inserted_at 은 넣지 않는다 → 서버의 DEFAULT now64(3).
     */
    static final Map<String, Function<LogEvent, Object>> COLUMNS;

    static {
        Map<String, Function<LogEvent, Object>> c = new LinkedHashMap<>();
        c.put("device_id", LogEvent::deviceId);
        c.put("source", LogEvent::source);
        c.put("event_time", LogEvent::eventTime);
        c.put("collected_at", LogEvent::collectedAt);
        c.put("offset", LogEvent::offset);
        c.put("raw", LogEvent::raw);
        c.put("category", LogEvent::category);
        c.put("level", LogEvent::level);
        c.put("message", LogEvent::message);
        COLUMNS = Collections.unmodifiableMap(c);
    }

    /** 묶음을 RowBinaryWithDefaults 로 쓴다. 클라이언트가 재시도할 때 다시 불릴 수 있으므로 events 만 읽고 상태를 남기지 않는다. */
    private void write(OutputStream out, List<LogEvent> events) throws IOException {
        RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, schema(), ClickHouseFormat.RowBinaryWithDefaults);
        for (LogEvent e : events) {
            for (Map.Entry<String, Function<LogEvent, Object>> col : COLUMNS.entrySet()) {
                w.setValue(col.getKey(), col.getValue().apply(e));
            }
            w.commitRow();
        }
    }
}
