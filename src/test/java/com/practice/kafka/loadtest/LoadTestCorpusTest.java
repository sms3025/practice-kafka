package com.practice.kafka.loadtest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 부하 테스트 입력을 읽는 규칙. Fluent Bit 이 읽는 파일만 고르고, 오프셋과 원문을 Fluent Bit 과 같은 기준으로 잡는지 본다.
 *
 * <p>예시 줄은 형식만 흉내 낸 가짜 내용이다. (실제 생성기 출력은 대외비이므로 저장소에 넣지 않는다)
 */
class LoadTestCorpusTest {

    @TempDir
    Path dir;

    @Test
    void picksOnlyFilesFluentBitWouldTail() {
        assertThat(LoadTestCorpus.relativeLogPath(Path.of("OHT-0007", "UILogger", "APP_20261012_14.log")))
                .isEqualTo("UILogger/APP_20261012_14.log");
        assertThat(LoadTestCorpus.relativeLogPath(Path.of("OHT-0007", "OHTHandlerLogger", "SEQ_INFO", "20261012_14_SEQ_INFO.txt")))
                .isEqualTo("OHTHandlerLogger/SEQ_INFO/20261012_14_SEQ_INFO.txt");
        assertThat(LoadTestCorpus.relativeLogPath(Path.of("OHT-0007", "format_guide.html"))).isNull();
        assertThat(LoadTestCorpus.relativeLogPath(Path.of("OHT-0007", "UILogger", "notes.txt"))).isNull();
    }

    @Test
    void readsCrlfLinesWithByteOffsetsAndSkipsEmptyLines() throws Exception {
        Path file = dir.resolve("APP.log");
        String line1 = "2026-10-12 14:03:21,123 [1] INFO APP - example one";
        String line2 = "2026-10-12 14:03:22,456 [1] WARN APP - 예시 둘";     // 한글은 UTF-8 로 3바이트씩
        Files.writeString(file, line1 + "\r\n\r\n" + line2 + "\r\n", StandardCharsets.UTF_8);

        List<LoadTestCorpus.Line> out = new ArrayList<>();
        long rawBytes = LoadTestCorpus.readLines(file, "UILogger/APP.log", out);

        assertThat(out).hasSize(2);                                          // CR 만 있는 빈 줄은 건너뛴다
        assertThat(out.get(0).offset()).isZero();
        assertThat(out.get(0).raw()).isEqualTo(line1 + "\r");               // CR 은 LogNormalizer 가 지운다
        long line2Offset = line1.getBytes(StandardCharsets.UTF_8).length + 4; // "\r\n\r\n"
        assertThat(out.get(1).offset()).isEqualTo(line2Offset);              // 문자 수가 아니라 바이트 위치
        assertThat(out.get(1).raw()).isEqualTo(line2 + "\r");
        assertThat(rawBytes).isEqualTo(line1.getBytes(StandardCharsets.UTF_8).length + 1
                + line2.getBytes(StandardCharsets.UTF_8).length + 1);
    }

    @Test
    void rebuildsSourceForVirtualVehicle() throws Exception {
        Path ui = Files.createDirectories(dir.resolve("OHT-0001").resolve("UILogger"));
        Files.writeString(ui.resolve("NET_20261012_14.log"), "2026-10-12 14:03:21,123 [1] INFO NET - example\r\n");
        Files.writeString(dir.resolve("OHT-0001").resolve("guide.html"), "<html></html>");

        LoadTestCorpus corpus = new LoadTestCorpus(dir.toString());

        assertThat(corpus.summary().files()).isEqualTo(1);
        assertThat(corpus.lines()).singleElement().satisfies(line ->
                assertThat(line.sourceFor("OHT-0423"))
                        .isEqualTo("/var/log/devices/OHT-0423/UILogger/NET_20261012_14.log"));
    }
}
