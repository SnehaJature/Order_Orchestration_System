package com.orderflow.messaging;

import java.util.Map;

public record SagaEvent(String type, String orderId, Map<String, Object> data) {
    public static final String ORDER_CREATED = "ORDER_CREATED";
    public static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";
    public static final String INVENTORY_FAILED = "INVENTORY_FAILED";
    public static final String PAYMENT_AUTHORIZED = "PAYMENT_AUTHORIZED";
    public static final String PAYMENT_FAILED = "PAYMENT_FAILED";
    public static final String SHIPMENT_CREATED = "SHIPMENT_CREATED";
    public static final String ORDER_COMPLETED = "ORDER_COMPLETED";
    public static final String COMPENSATE_PAYMENT = "COMPENSATE_PAYMENT";
    public static final String COMPENSATE_INVENTORY = "COMPENSATE_INVENTORY";
}