package com.practice.kafka.ingest;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;

/**
 * gzip 으로 압축된 요청 본문을 풀어 컨트롤러에 넘긴다.
 *
 * <p>Fluent Bit 의 http 출력에 {@code compress gzip} 을 켜면 본문을 gzip 으로 압축하고 {@code Content-Encoding: gzip}
 * 헤더를 붙인다(명세서의 "전송 최적화"). Spring MVC(Tomcat)는 응답 압축은 해 주지만 요청 본문은 풀지 않으므로,
 * 이 필터가 없으면 컨트롤러의 JSON 읽기가 압축된 바이트를 만나 400 으로 실패한다.
 *
 * <p>헤더가 없거나 gzip 이 아니면 아무것도 하지 않는다. 그래서 압축하는 에이전트와 하지 않는 에이전트가 섞여 있어도 된다.
 */
@Component
public class GzipRequestFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String encoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
        if (encoding != null && encoding.trim().equalsIgnoreCase("gzip")) {
            chain.doFilter(new GunzippedRequest(request), response);
        } else {
            chain.doFilter(request, response);
        }
    }

    /**
     * 본문을 풀어서 읽게 하는 요청. 풀고 나면 길이가 달라지므로 Content-Length 와 Content-Encoding 헤더는 감춘다.
     * 그대로 두면 뒤의 읽기 코드가 압축된 길이만큼만 읽으려 하거나 다시 풀려고 할 수 있다.
     */
    static final class GunzippedRequest extends HttpServletRequestWrapper {

        private ServletInputStream body;

        GunzippedRequest(HttpServletRequest request) {
            super(request);
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            if (body == null) {
                body = new GunzipStream(new GZIPInputStream(super.getInputStream(), 64 * 1024));
            }
            return body;
        }

        @Override
        public int getContentLength() {
            return -1;
        }

        @Override
        public long getContentLengthLong() {
            return -1;
        }

        @Override
        public String getHeader(String name) {
            return hidden(name) ? null : super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            return hidden(name) ? Collections.emptyEnumeration() : super.getHeaders(name);
        }

        @Override
        public Enumeration<String> getHeaderNames() {
            return Collections.enumeration(Collections.list(super.getHeaderNames()).stream()
                    .filter(n -> !hidden(n))
                    .toList());
        }

        private static boolean hidden(String name) {
            return HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(name) || HttpHeaders.CONTENT_ENCODING.equalsIgnoreCase(name);
        }
    }

    /** GZIPInputStream 을 서블릿 입력 스트림 모양으로 감싼다. 동기 읽기만 쓰므로 비동기 메서드는 지원하지 않는다. */
    private static final class GunzipStream extends ServletInputStream {

        private final InputStream in;
        private boolean finished;

        GunzipStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            finished = b < 0;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int n = in.read(buf, off, len);
            finished = n < 0;
            return n;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener listener) {
            throw new UnsupportedOperationException("비동기 읽기는 지원하지 않는다");
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
