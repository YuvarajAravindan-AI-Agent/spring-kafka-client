package com.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Bounded in-memory buffer of the most recently consumed records,
 * so the demo UI can show the produce -> consume round trip.
 */
@Component
public class RecentOrders {

    private static final int MAX = 50;

    private final Deque<Map<String, Object>> recent = new ConcurrentLinkedDeque<>();

    public void add(ConsumerRecord<String, String> record) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("partition", record.partition());
        entry.put("offset", record.offset());
        entry.put("key", record.key());
        entry.put("value", record.value());
        entry.put("consumedAt", Instant.now().toString());
        recent.addFirst(entry);
        while (recent.size() > MAX) {
            recent.pollLast();
        }
    }

    public List<Map<String, Object>> list() {
        return new ArrayList<>(recent);
    }
}
