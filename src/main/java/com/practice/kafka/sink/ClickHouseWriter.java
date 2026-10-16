package com.practice.kafka.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import com.practice.kafka.log.LogEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 로그 묶음을 ClickHouse 에 INSERT 한 번으로 넣는다. 형식은 RowBinaryWithDefaults 다.
 *
 * <p>RowBinary 는 ClickHouse 의 바이너리 행 형식이다. 문자열은 길이 + 바이트, 시각은 정수로 보내므로 서버가 JSON 처럼 글자를 읽어
 * 해석할 필요가 없다. 대신 그 변환을 이 쪽(컨슈머)이 한다. "WithDefaults" 는 값을 비운 컬럼에 테이블의 DEFAULT 를 쓰라는 표시를
 * 보낼 수 있는 변형이다. {@code inserted_at} 을 비워 두어 ClickHouse 가 기록하는 순간의 시각({@code now64(3)})이 들어가게 한다.
 *
 * <p>INSERT 한 번은 ClickHouse 디스크에 part 하나를 만든다(파티션이 하나일 때). 그래서 묶음이 작고 잦으면 part 가 쌓여 병합이 밀린다.
 * 묶음 크기는 리스너의 {@code max.poll.records} 와 fetch 설정이 정한다.
 */
@Component
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class ClickHouseWriter {

    private final Client client;
    private final String table;
    private final SinkStats stats;
    private final TableSchema schema;

    public ClickHouseWriter(Client client, @Value("${app.clickhouse.table}") String table, SinkStats stats) {
        this.client = client;
        this.table = table;
        this.stats = stats;
        // 컬럼 순서와 타입을 서버에서 한 번 읽어 둔다. RowBinary 는 컬럼 이름 없이 순서대로 값을 보내기 때문이다.
        this.schema = client.getTableSchema(table);
    }

    /** events 를 INSERT 한 번으로 넣는다. 실패하면 예외를 던지고, 리스너의 에러 핸들러가 같은 묶음을 다시 시도한다. */
    public void insert(List<LogEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        long startMs = System.currentTimeMillis();
        long start = System.nanoTime();
        try (InsertResponse response = client.insert(table, out -> write(out, events),
                ClickHouseFormat.RowBinaryWithDefaults, new InsertSettings()).get(120, TimeUnit.SECONDS)) {
            stats.recordInsert(startMs, events.size(), response.getWrittenBytes(), System.nanoTime() - start,
                    response.getServerTime());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stats.recordFailure();
            throw new IllegalStateException("ClickHouse INSERT 중 인터럽트", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            stats.recordFailure();
            throw new IllegalStateException("ClickHouse INSERT 실패 (" + events.size() + "건)", e);
        }
    }

    /**
     * 묶음을 RowBinaryWithDefaults 로 쓴다. 클라이언트가 재시도할 때 다시 불릴 수 있으므로 events 만 읽고 상태를 남기지 않는다.
     * 컬럼 이름은 테이블(docker/clickhouse/init/01-schema.sql)과 같아야 한다.
     */
    private void write(OutputStream out, List<LogEvent> events) throws IOException {
        RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, schema, ClickHouseFormat.RowBinaryWithDefaults);
        for (LogEvent e : events) {
            w.setValue("device_id", e.deviceId());
            w.setValue("source", e.source());
            w.setValue("event_time", e.eventTime());
            w.setValue("collected_at", e.collectedAt());
            w.setValue("offset", e.offset());
            w.setValue("raw", e.raw());
            w.setValue("category", e.category());
            w.setValue("level", e.level());
            w.setValue("message", e.message());
            // inserted_at 은 비워 둔다 → 서버의 DEFAULT now64(3)
            w.commitRow();
        }
    }
}
