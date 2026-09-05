package com.orderflow.messaging;

import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
public class RetryExecutor {
    public <T> T execute(String operation, Supplier<T> action) {
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                return action.get();
            } catch (RuntimeException failure) {
                lastFailure = failure;
                if (attempt < 3) {
                    try {
                        Thread.sleep(100L * attempt);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(operation + " interrupted", interrupted);
                    }
                }
            }
        }
        throw new IllegalStateException(operation + " failed after retries", lastFailure);
    }
}