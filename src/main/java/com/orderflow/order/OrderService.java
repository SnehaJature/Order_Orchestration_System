package com.orderflow.order;

import com.orderflow.api.CreateOrderRequest;
import com.orderflow.api.OrderResponse;
import com.orderflow.messaging.EventPublisher;
import com.orderflow.messaging.EventTopics;
import com.orderflow.messaging.SagaEvent;
import com.orderflow.saga.LocalSagaFallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class OrderService {
    private final JdbcTemplate jdbcTemplate;
    private final EventPublisher publisher;
    private final ObjectProvider<LocalSagaFallback> localSagaFallback;

    public OrderService(JdbcTemplate jdbcTemplate, EventPublisher publisher, ObjectProvider<LocalSagaFallback> localSagaFallback) {
        this.jdbcTemplate = jdbcTemplate;
        this.publisher = publisher;
        this.localSagaFallback = localSagaFallback;
    }

    public OrderResponse create(CreateOrderRequest request) {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        double total = request.items().stream().mapToDouble(item -> item.price() * item.quantity()).sum();
        jdbcTemplate.update("INSERT INTO orders(order_id, customer_id, status, total) VALUES (?, ?, ?, ?)",
                orderId, request.customerId(), "PENDING", total);
        Map<String, Object> data = Map.of(
                "customerId", request.customerId(),
                "items", request.items(),
                "total", total,
                "forcePaymentFailure", request.forcePaymentFailure(),
                "forceInventoryFailure", request.forceInventoryFailure());
        publisher.publish(EventTopics.ORDER, new SagaEvent(SagaEvent.ORDER_CREATED, orderId, data));
        localSagaFallback.ifAvailable(fallback -> fallback.process(orderId, request));
        return get(orderId);
    }

    public List<OrderResponse> list() {
        return jdbcTemplate.query("SELECT order_id, customer_id, status, total FROM orders ORDER BY created_at DESC",
                (resultSet, row) -> response(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getDouble(4)));
    }

    public OrderResponse get(String orderId) {
        return jdbcTemplate.queryForObject("SELECT order_id, customer_id, status, total FROM orders WHERE order_id = ?",
                (resultSet, row) -> response(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getDouble(4)), orderId);
    }

    public void recordEvent(SagaEvent event) {
        jdbcTemplate.update("INSERT INTO saga_events(order_id, event_type) VALUES (?, ?)", event.orderId(), event.type());
    }

    public void updateStatus(String orderId, String status) {
        jdbcTemplate.update("UPDATE orders SET status = ? WHERE order_id = ?", status, orderId);
    }

    private OrderResponse response(String orderId, String customerId, String status, double total) {
        List<String> events = jdbcTemplate.query("SELECT event_type FROM saga_events WHERE order_id = ? ORDER BY created_at",
                (resultSet, row) -> resultSet.getString(1), orderId);
        return new OrderResponse(orderId, customerId, status, total, events);
    }
}