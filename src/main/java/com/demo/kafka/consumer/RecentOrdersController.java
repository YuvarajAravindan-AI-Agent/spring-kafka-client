package com.demo.kafka.consumer;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class RecentOrdersController {

    private final RecentOrders recentOrders;

    public RecentOrdersController(RecentOrders recentOrders) {
        this.recentOrders = recentOrders;
    }

    @GetMapping("/recent")
    public List<Map<String, Object>> recent() {
        return recentOrders.list();
    }
}
