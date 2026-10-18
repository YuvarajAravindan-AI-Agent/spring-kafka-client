package com.demo.kafka;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-IP rate limit for the public demo.
 *
 * The endpoint is deliberately open — anyone should be able to try the
 * cluster without credentials — so this is the only thing standing between
 * the demo and someone filling the topic. Writes are capped harder than
 * reads because writes are what hit Kafka.
 *
 * Fixed window, in memory: the app is a single instance and a coarse
 * limit is enough here. A shared store would be the answer if this ever
 * ran replicated.
 */
@Component
@Order(1)
public class RateLimitFilter extends OncePerRequestFilter {

    private static final int WRITE_LIMIT_PER_MINUTE = 10;
    private static final int READ_LIMIT_PER_MINUTE = 60;
    private static final Duration WINDOW = Duration.ofMinutes(1);
    private static final int MAX_TRACKED_IPS = 10_000;

    private record Counter(Instant windowStart, AtomicInteger count) {}

    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {

        int limit = "POST".equalsIgnoreCase(request.getMethod())
                ? WRITE_LIMIT_PER_MINUTE
                : READ_LIMIT_PER_MINUTE;

        if (exceeded(clientIp(request), limit)) {
            response.setStatus(429);
            response.setContentType("application/json");
            response.getWriter().write(
                    "{\"error\":\"rate limit exceeded\",\"limit\":\"" + limit
                            + " requests/minute\",\"note\":\"public demo - please go easy\"}");
            return;
        }

        chain.doFilter(request, response);
    }

    private boolean exceeded(String ip, int limit) {
        Instant now = Instant.now();

        // Cheap guard against unbounded growth from spoofed/rotating IPs.
        if (counters.size() > MAX_TRACKED_IPS) {
            counters.entrySet().removeIf(e -> isStale(e.getValue(), now));
        }

        Counter counter = counters.compute(ip, (key, existing) ->
                (existing == null || isStale(existing, now))
                        ? new Counter(now, new AtomicInteger(0))
                        : existing);

        return counter.count().incrementAndGet() > limit;
    }

    private boolean isStale(Counter counter, Instant now) {
        return Duration.between(counter.windowStart(), now).compareTo(WINDOW) >= 0;
    }

    /** Caddy terminates TLS, so the real client address arrives in X-Forwarded-For. */
    private String clientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            int comma = forwarded.indexOf(',');
            return (comma > 0 ? forwarded.substring(0, comma) : forwarded).trim();
        }
        return request.getRemoteAddr();
    }
}
