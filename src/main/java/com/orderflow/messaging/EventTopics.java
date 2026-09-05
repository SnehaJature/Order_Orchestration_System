package com.orderflow.messaging;

public final class EventTopics {
    public static final String ORDER = "order.events";
    public static final String INVENTORY = "inventory.events";
    public static final String PAYMENT = "payment.events";
    public static final String SHIPPING = "shipping.events";

    private EventTopics() {
    }
}