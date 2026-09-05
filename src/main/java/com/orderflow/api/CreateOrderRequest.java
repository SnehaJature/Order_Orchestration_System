package com.orderflow.api;

import java.util.List;

public record CreateOrderRequest(
        String customerId,
        List<OrderLine> items,
        boolean forcePaymentFailure,
        boolean forceInventoryFailure) {
}