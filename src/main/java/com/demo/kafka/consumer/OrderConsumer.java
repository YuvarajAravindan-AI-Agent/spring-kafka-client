package com.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final RecentOrders recentOrders;

    public OrderConsumer(RecentOrders recentOrders) {
        this.recentOrders = recentOrders;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "demo-consumer-group")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Received partition={} offset={} key={} value={}",
                record.partition(), record.offset(), record.key(), record.value());
        recentOrders.add(record);
        ack.acknowledge();
    }
}
