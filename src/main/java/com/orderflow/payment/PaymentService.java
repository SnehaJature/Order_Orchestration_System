package com.orderflow.payment;

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
public class PaymentService {
    private final ObjectMapper objectMapper;
    private final EventPublisher publisher;
    private final IdempotencyStore idempotency;
    private final RetryExecutor retries;

    public PaymentService(ObjectMapper objectMapper, EventPublisher publisher, IdempotencyStore idempotency, RetryExecutor retries) {
        this.objectMapper = objectMapper;
        this.publisher = publisher;
        this.idempotency = idempotency;
        this.retries = retries;
    }

    @KafkaListener(topics = EventTopics.INVENTORY, groupId = "payment-service")
    public void consume(String payload) {
        SagaEvent event = EventJson.read(objectMapper, payload);
        if (!SagaEvent.INVENTORY_RESERVED.equals(event.type()) || !idempotency.firstDelivery("payment", event.orderId() + ":authorize")) {
            return;
        }
        retries.execute("payment authorization", () -> {
            publisher.publish(EventTopics.PAYMENT, new SagaEvent(
                    Boolean.TRUE.equals(event.data().get("forcePaymentFailure")) ? SagaEvent.PAYMENT_FAILED : SagaEvent.PAYMENT_AUTHORIZED,
                    event.orderId(), Map.of("reason", "Payment authorization result")));
            return null;
        });
    }

    @KafkaListener(topics = EventTopics.PAYMENT, groupId = "payment-compensation")
    public void compensate(String payload) {
        SagaEvent event = EventJson.read(objectMapper, payload);
        if (SagaEvent.COMPENSATE_PAYMENT.equals(event.type()) && idempotency.firstDelivery("payment", event.orderId() + ":refund")) {
            publisher.publish(EventTopics.PAYMENT, new SagaEvent("PAYMENT_REFUNDED", event.orderId(), Map.of("reason", "Saga compensation")));
        }
    }
}