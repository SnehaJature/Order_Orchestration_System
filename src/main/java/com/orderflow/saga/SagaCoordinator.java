package com.orderflow.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.messaging.EventJson;
import com.orderflow.messaging.EventPublisher;
import com.orderflow.messaging.EventTopics;
import com.orderflow.messaging.IdempotencyStore;
import com.orderflow.messaging.SagaEvent;
import com.orderflow.order.OrderService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class SagaCoordinator {
    private final ObjectMapper objectMapper;
    private final EventPublisher publisher;
    private final IdempotencyStore idempotency;
    private final OrderService orderService;

    public SagaCoordinator(ObjectMapper objectMapper, EventPublisher publisher, IdempotencyStore idempotency, OrderService orderService) {
        this.objectMapper = objectMapper;
        this.publisher = publisher;
        this.idempotency = idempotency;
        this.orderService = orderService;
    }

    @KafkaListener(topics = {EventTopics.ORDER, EventTopics.INVENTORY, EventTopics.PAYMENT, EventTopics.SHIPPING}, groupId = "saga-coordinator")
    public void coordinate(String payload) {
        SagaEvent event = EventJson.read(objectMapper, payload);
        if (!idempotency.firstDelivery("coordinator", event.orderId() + ":" + event.type())) {
            return;
        }
        orderService.recordEvent(event);
        switch (event.type()) {
            case SagaEvent.ORDER_CREATED -> orderService.updateStatus(event.orderId(), "INVENTORY_PENDING");
            case SagaEvent.INVENTORY_RESERVED -> orderService.updateStatus(event.orderId(), "PAYMENT_PENDING");
            case SagaEvent.INVENTORY_FAILED -> orderService.updateStatus(event.orderId(), "CANCELLED_INVENTORY");
            case SagaEvent.PAYMENT_AUTHORIZED -> orderService.updateStatus(event.orderId(), "SHIPPING_PENDING");
            case SagaEvent.PAYMENT_FAILED -> compensatePayment(event.orderId());
            case SagaEvent.SHIPMENT_CREATED -> {
                orderService.updateStatus(event.orderId(), "COMPLETED");
                publisher.publish(EventTopics.ORDER, new SagaEvent(SagaEvent.ORDER_COMPLETED, event.orderId(), Map.of()));
            }
            default -> {
                if (event.type().endsWith("RELEASED") || event.type().endsWith("REFUNDED")) {
                    orderService.recordEvent(event);
                }
            }
        }
    }

    private void compensatePayment(String orderId) {
        orderService.updateStatus(orderId, "COMPENSATING");
        publisher.publish(EventTopics.PAYMENT, new SagaEvent(SagaEvent.COMPENSATE_PAYMENT, orderId, Map.of()));
        publisher.publish(EventTopics.INVENTORY, new SagaEvent(SagaEvent.COMPENSATE_INVENTORY, orderId, Map.of()));
        orderService.updateStatus(orderId, "CANCELLED_PAYMENT");
    }
}