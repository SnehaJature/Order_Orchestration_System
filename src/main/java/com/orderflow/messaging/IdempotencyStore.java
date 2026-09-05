package com.orderflow.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class IdempotencyStore {
    private final JdbcTemplate jdbcTemplate;

    public IdempotencyStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean firstDelivery(String consumer, String eventKey) {
        try {
            return jdbcTemplate.update(
                    "INSERT INTO processed_events(consumer_name, event_key) VALUES (?, ?)", consumer, eventKey) == 1;
        } catch (RuntimeException duplicate) {
            return false;
        }
    }
}