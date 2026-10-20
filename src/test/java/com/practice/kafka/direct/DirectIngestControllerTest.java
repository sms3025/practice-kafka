package com.practice.kafka.direct;

import com.practice.kafka.ingest.FluentBitRecord;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import com.practice.kafka.sink.ClickHouseWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 직접 적재 경로(Step 4+)의 응답 계약을 ClickHouse 없이 확인한다.
 * 저장소가 받은 뒤에만 200, 실패하면 500(Fluent Bit 재전송), async 파라미터로 테이블(=INSERT 방식)이 갈린다.
 */
class DirectIngestControllerTest {

    private static final Instant COLLECTED = Instant.parse("2026-10-19T01:00:00Z");

    private final ClickHouseWriter direct = mock(ClickHouseWriter.class);
    private final ClickHouseWriter directAsync = mock(ClickHouseWriter.class);
    private DirectIngestController controller;

    @BeforeEach
    void setUp() {
        when(direct.table()).thenReturn("app_logs_direct");
        when(directAsync.table()).thenReturn("app_logs_direct_async");
        controller = new DirectIngestController(new LogNormalizer("Asia/Seoul", "([A-Z]{2,5}-[0-9]{3,5})"), direct, directAsync);
    }

    // 형식만 맞춘 가짜 줄이다. 실제 장비 로그가 아니다.
    private static List<FluentBitRecord> batch() {
        return List.of(
                new FluentBitRecord(COLLECTED, "2026-10-19 10:00:00.000 [INFO] DEMO sample line one", "/var/log/devices/VH-0001/a.log", 0L),
                new FluentBitRecord(COLLECTED, "2026-10-19 10:00:01.000 [WARN] DEMO sample line two", "/var/log/devices/VH-0001/a.log", 53L));
    }

    @SuppressWarnings("unchecked")
    @Test
    void normalizesBatchAndInsertsOnceThenReturns200() {
        var response = controller.ingest(batch(), false);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        ArgumentCaptor<List<LogEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(direct).insert(captor.capture());              // 묶음 하나 = INSERT 한 번
        verify(directAsync, never()).insert(anyList());
        assertThat(captor.getValue()).extracting(LogEvent::deviceId, LogEvent::offset, LogEvent::level)
                .containsExactly(tuple("VH-0001", 0L, "INFO"),
                        tuple("VH-0001", 53L, "WARN"));
        assertThat(stats("app_logs_direct")).containsEntry("inserts", 1L).containsEntry("rows", 2L).containsEntry("failures", 0L);
    }

    @Test
    void asyncParameterRoutesToAsyncInsertWriter() {
        var response = controller.ingest(batch(), true);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(directAsync).insert(anyList());
        verify(direct, never()).insert(anyList());
        assertThat(stats("app_logs_direct_async")).containsEntry("rows", 2L);
    }

    @Test
    void storageFailureReturns500SoAgentRetries() {
        doThrow(new IllegalStateException("ClickHouse INSERT 실패")).when(direct).insert(anyList());

        var response = controller.ingest(batch(), false);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
        assertThat(stats("app_logs_direct")).containsEntry("inserts", 0L).containsEntry("failures", 1L);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Long> stats(String table) {
        return (Map<String, Long>) controller.stats().get(table);
    }
}
