package com.orderflow.shipping;

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
public class ShippingService {
    private final ObjectMapper objectMapper;
    private final EventPublisher publisher;
    private final IdempotencyStore idempotency;
    private final RetryExecutor retries;

    public ShippingService(ObjectMapper objectMapper, EventPublisher publisher, IdempotencyStore idempotency, RetryExecutor retries) {
        this.objectMapper = objectMapper;
        this.publisher = publisher;
        this.idempotency = idempotency;
        this.retries = retries;
    }

    @KafkaListener(topics = EventTopics.PAYMENT, groupId = "shipping-service")
    public void consume(String payload) {
        SagaEvent event = EventJson.read(objectMapper, payload);
        if (!SagaEvent.PAYMENT_AUTHORIZED.equals(event.type()) || !idempotency.firstDelivery("shipping", event.orderId() + ":create")) {
            return;
        }
        retries.execute("shipment creation", () -> {
            publisher.publish(EventTopics.SHIPPING, new SagaEvent(
                    SagaEvent.SHIPMENT_CREATED, event.orderId(), Map.of("reason", "Shipment created")));
            return null;
        });
    }
}