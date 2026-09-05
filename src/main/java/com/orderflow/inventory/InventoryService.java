package com.orderflow.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.messaging.EventJson;
import com.orderflow.messaging.EventPublisher;
import com.orderflow.messaging.EventTopics;
import com.orderflow.messaging.IdempotencyStore;
import com.orderflow.messaging.RetryExecutor;
import com.orderflow.messaging.SagaEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class InventoryService {
    private final ObjectMapper objectMapper;
    private final EventPublisher publisher;
    private final IdempotencyStore idempotency;
    private final RetryExecutor retries;

    public InventoryService(ObjectMapper objectMapper, EventPublisher publisher, IdempotencyStore idempotency, RetryExecutor retries) {
        this.objectMapper = objectMapper;
        this.publisher = publisher;
        this.idempotency = idempotency;
        this.retries = retries;
    }

    @KafkaListener(topics = EventTopics.ORDER, groupId = "inventory-service")
    public void consume(String payload) {
        SagaEvent event = EventJson.read(objectMapper, payload);
        if (!SagaEvent.ORDER_CREATED.equals(event.type()) || !idempotency.firstDelivery("inventory", event.orderId() + ":reserve")) {
            return;
        }
        boolean failed = Boolean.TRUE.equals(event.data().get("forceInventoryFailure"));
        retries.execute("inventory reservation", () -> {
            publisher.publish(EventTopics.INVENTORY, new SagaEvent(
                    failed ? SagaEvent.INVENTORY_FAILED : SagaEvent.INVENTORY_RESERVED,
                event.orderId(), Map.of(
                    "reason", failed ? "Insufficient stock" : "Inventory reserved",
                    "forcePaymentFailure", event.data().getOrDefault("forcePaymentFailure", false))));
            return null;
        });
    }

    @KafkaListener(topics = EventTopics.INVENTORY, groupId = "inventory-compensation")
    public void compensate(String payload) {
        SagaEvent event = EventJson.read(objectMapper, payload);
        if (SagaEvent.COMPENSATE_INVENTORY.equals(event.type()) && idempotency.firstDelivery("inventory", event.orderId() + ":release")) {
            publisher.publish(EventTopics.INVENTORY, new SagaEvent("INVENTORY_RELEASED", event.orderId(), Map.of("reason", "Saga compensation")));
        }
    }
}