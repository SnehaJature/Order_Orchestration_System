package com.orderflow.saga;

import com.orderflow.api.CreateOrderRequest;
import com.orderflow.messaging.SagaEvent;
import com.orderflow.order.OrderService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.saga.local-fallback", havingValue = "true")
public class LocalSagaFallback {
    private final OrderService orderService;

    public LocalSagaFallback(OrderService orderService) {
        this.orderService = orderService;
    }

    public void process(String orderId, CreateOrderRequest request) {
        event(orderId, SagaEvent.ORDER_CREATED);
        orderService.updateStatus(orderId, "INVENTORY_PENDING");

        if (request.forceInventoryFailure()) {
            event(orderId, SagaEvent.INVENTORY_FAILED);
            orderService.updateStatus(orderId, "CANCELLED_INVENTORY");
            return;
        }

        event(orderId, SagaEvent.INVENTORY_RESERVED);
        orderService.updateStatus(orderId, "PAYMENT_PENDING");
        if (request.forcePaymentFailure()) {
            event(orderId, SagaEvent.PAYMENT_FAILED);
            orderService.updateStatus(orderId, "COMPENSATING");
            event(orderId, SagaEvent.COMPENSATE_PAYMENT);
            event(orderId, "PAYMENT_REFUNDED");
            event(orderId, SagaEvent.COMPENSATE_INVENTORY);
            event(orderId, "INVENTORY_RELEASED");
            orderService.updateStatus(orderId, "CANCELLED_PAYMENT");
            return;
        }

        event(orderId, SagaEvent.PAYMENT_AUTHORIZED);
        orderService.updateStatus(orderId, "SHIPPING_PENDING");
        event(orderId, SagaEvent.SHIPMENT_CREATED);
        orderService.updateStatus(orderId, "COMPLETED");
        event(orderId, SagaEvent.ORDER_COMPLETED);
    }

    private void event(String orderId, String type) {
        orderService.recordEvent(new SagaEvent(type, orderId, Map.of("source", "local-fallback")));
    }
}