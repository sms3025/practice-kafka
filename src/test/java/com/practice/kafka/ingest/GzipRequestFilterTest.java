package com.practice.kafka.ingest;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fluent Bit 의 compress gzip 본문을 풀어 컨트롤러가 평문 JSON 으로 읽게 하는지, 압축 안 된 요청은 건드리지 않는지 본다.
 */
class GzipRequestFilterTest {

    private static final String JSON = "[{\"log\":\"2026-10-12 14:03:21,123 [1] INFO APP - example\",\"offset\":0}]";

    @Test
    void gzipBodyIsUnzippedAndEncodingHeadersAreHidden() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/ingest/fluentbit");
        byte[] gz = gzip(JSON);
        request.setContent(gz);
        request.addHeader("Content-Encoding", "gzip");
        request.addHeader("Content-Length", String.valueOf(gz.length));
        MockFilterChain chain = new MockFilterChain();

        new GzipRequestFilter().doFilter(request, new MockHttpServletResponse(), chain);

        HttpServletRequest seen = (HttpServletRequest) chain.getRequest();
        assertThat(new String(seen.getInputStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(JSON);
        assertThat(seen.getHeader("Content-Encoding")).isNull();            // 뒤에서 다시 풀려고 하지 않게
        assertThat(seen.getHeader("Content-Length")).isNull();              // 압축된 길이만큼만 읽지 않게
        assertThat(seen.getContentLength()).isEqualTo(-1);
        assertThat(Collections.list(seen.getHeaderNames())).doesNotContain("Content-Encoding", "Content-Length");
    }

    @Test
    void plainBodyPassesThroughUntouched() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/ingest/fluentbit");
        request.setContent(JSON.getBytes(StandardCharsets.UTF_8));
        MockFilterChain chain = new MockFilterChain();

        new GzipRequestFilter().doFilter(request, new MockHttpServletResponse(), chain);

        assertThat(chain.getRequest()).isSameAs(request);
    }

    private static byte[] gzip(String s) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(buf)) {
            out.write(s.getBytes(StandardCharsets.UTF_8));
        }
        return buf.toByteArray();
    }
}
