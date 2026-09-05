package com.orderflow.api;

import java.util.List;

public record OrderResponse(
        String orderId,
        String customerId,
        String status,
        double total,
        List<String> events) {
}