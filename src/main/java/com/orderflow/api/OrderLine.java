package com.orderflow.api;

public record OrderLine(String sku, int quantity, double price) {
}