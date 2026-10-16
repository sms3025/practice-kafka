package com.practice.kafka.loadtest;

import com.practice.kafka.log.LogNormalizer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fluent Bit 이 보냈을 요청을 재현하는 규칙: (초, 파일)마다 요청 하나, 파일 안 순서 유지, 시각 없는 줄은 앞 줄의 초.
 *
 * <p>예시 줄은 형식만 흉내 낸 가짜 내용이다. (실제 생성기 출력은 대외비이므로 저장소에 넣지 않는다)
 */
class FluentBitBatchesTest {

    @TempDir
    Path dir;

    private final LogNormalizer normalizer = new LogNormalizer("Asia/Seoul", "([A-Z]{2,5}-[0-9]{3,5})");

    @Test
    void groupsLinesBySecondAndFile() throws Exception {
        Path ui = Files.createDirectories(dir.resolve("OHT-0001").resolve("UILogger"));
        Files.writeString(ui.resolve("APP_20261012_14.log"), String.join("\r\n",
                "2026-10-12 14:03:21,100 [1] INFO APP - example one",
                "2026-10-12 14:03:21,900 [1] INFO APP - example two",
                "continued line without a timestamp",
                "2026-10-12 14:03:22,050 [1] WARN APP - example three") + "\r\n");
        Files.writeString(ui.resolve("NET_20261012_14.log"),
                "2026-10-12 14:03:21,500 [1] INFO NET - example net\r\n");

        FluentBitBatches batches = new FluentBitBatches(new LoadTestCorpus(dir.toString()), normalizer,
                dir.resolve("out").resolve("payload.jsonl").toString());
        Map<Long, Map<String, List<LoadTestCorpus.Line>>> bySecond =
                batches.groupBySecond(new LoadTestCorpus(dir.toString()).lines());

        assertThat(bySecond).hasSize(2);                                   // 14:03:21 과 14:03:22
        var first = bySecond.values().iterator().next();
        assertThat(first).containsOnlyKeys("UILogger/APP_20261012_14.log", "UILogger/NET_20261012_14.log");
        // 같은 초의 APP 줄 셋(시각 없는 이어진 줄 포함)이 파일 안 순서대로 한 요청에 들어간다
        assertThat(first.get("UILogger/APP_20261012_14.log")).extracting(LoadTestCorpus.Line::raw)
                .containsExactly("2026-10-12 14:03:21,100 [1] INFO APP - example one\r",
                        "2026-10-12 14:03:21,900 [1] INFO APP - example two\r",
                        "continued line without a timestamp\r");
    }

    @Test
    void perFileModeSendsOneRequestPerFilePerSecond() throws Exception {
        Path ui = Files.createDirectories(dir.resolve("OHT-0001").resolve("UILogger"));
        Files.writeString(ui.resolve("APP_20261012_14.log"), "2026-10-12 14:03:21,100 [1] INFO APP - a\r\n");
        Files.writeString(ui.resolve("OPS_20261012_14.log"), "2026-10-12 14:03:21,200 [1] INFO OPS - b\r\n");

        FluentBitBatches batches = new FluentBitBatches(new LoadTestCorpus(dir.toString()), normalizer,
                dir.resolve("out").resolve("payload.jsonl").toString());

        FluentBitBatches.Written perFile = batches.write(FluentBitBatches.Mode.PER_FILE, 10);
        FluentBitBatches.Written perDevice = batches.write(FluentBitBatches.Mode.PER_DEVICE, 10);

        assertThat(perFile.requests()).isEqualTo(2);
        assertThat(perFile.requestsPerDeviceSecond()).isEqualTo(2.0);
        assertThat(perDevice.requests()).isEqualTo(1);
        assertThat(perDevice.linesPerRequest()).isEqualTo(2.0);
        // 한 줄 = 요청 하나, Fluent Bit 형식의 JSON 배열
        assertThat(Files.readAllLines(Path.of(perFile.file()))).hasSize(2)
                .allSatisfy(l -> assertThat(l).startsWith("[{\"log\":").contains("\"source\":\"/var/log/devices/OHT-0001/"));
    }
}
