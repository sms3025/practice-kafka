package com.practice.kafka.sink;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 적재 코드가 쓰는 컬럼과 ClickHouse 테이블 DDL 의 컬럼이 같은지 본다 (Step 4).
 *
 * <p>RowBinary 는 컬럼 이름이 아니라 순서와 타입으로 값을 보내므로, 한쪽만 고치면 INSERT 가 실패하거나 엉뚱한 컬럼에 값이 들어간다.
 * ClickHouse 를 띄우지 않고도 그 어긋남을 잡기 위해 DDL 파일을 직접 읽는다. 스키마를 바꾸면 이 테스트, DDL, {@link ClickHouseWriter} 를 함께 고친다.
 */
class ClickHouseWriterColumnsTest {

    /** CREATE TABLE ( ... ) 안에서 줄 첫머리의 컬럼 이름. `offset` 처럼 백틱으로 감싼 이름도 읽는다. */
    private static final Pattern COLUMN = Pattern.compile("^\\s{4}`?([a-z_]+)`?\\s+(?:LowCardinality|String|DateTime64|Int64)");

    @Test
    void writerColumnsMatchTableDdlExceptServerDefault() throws Exception {
        List<String> ddlColumns = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of("docker/clickhouse/init/01-schema.sql"))) {
            Matcher m = COLUMN.matcher(line);
            if (m.find()) {
                ddlColumns.add(m.group(1));
            }
        }

        assertThat(ddlColumns).last().isEqualTo("inserted_at");                       // 서버가 DEFAULT 로 채우는 컬럼
        assertThat(ClickHouseWriter.COLUMNS.keySet())
                .containsExactlyElementsOf(ddlColumns.subList(0, ddlColumns.size() - 1)); // 나머지는 같은 이름, 같은 순서
    }

    /**
     * Step 4+ 의 Kafka 테이블 엔진은 토픽의 JSON 을 컬럼 이름으로 읽는다. 큐 테이블의 컬럼이 LogEvent 의 JSON 필드(= 적재 컬럼)와
     * 다르면 그 필드는 조용히 기본값('' 또는 0)으로 들어가므로, 이름이 어긋나지 않는지 여기서 본다.
     */
    @Test
    void kafkaEngineQueueColumnsMatchLogEventJsonFields() throws Exception {
        List<String> queueColumns = new ArrayList<>();
        boolean inQueue = false;
        for (String line : Files.readAllLines(Path.of("docker/clickhouse/compare/01-tables.sql"))) {
            if (line.contains("CREATE TABLE IF NOT EXISTS logs.app_logs_kafka_queue")) {
                inQueue = true;
            } else if (inQueue && line.startsWith("ENGINE")) {
                break;
            } else if (inQueue) {
                Matcher m = QUEUE_COLUMN.matcher(line);
                if (m.find()) {
                    queueColumns.add(m.group(1));
                }
            }
        }

        assertThat(queueColumns).containsExactlyElementsOf(ClickHouseWriter.COLUMNS.keySet());
    }

    private static final Pattern QUEUE_COLUMN = Pattern.compile("^\\s{4}`?([a-z_]+)`?\\s+(?:String|Int64|Nullable)");
}
