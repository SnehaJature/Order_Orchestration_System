package com.orderflow.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class EventJson {
    private EventJson() {
    }

    public static SagaEvent read(ObjectMapper mapper, String payload) {
        try {
            return mapper.readValue(payload, SagaEvent.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid saga event", exception);
        }
    }
}