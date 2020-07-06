package com.suke.hystrix;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;

@Getter
public abstract class HystrixCommandProperties {
    private  AtomicBoolean circuitBreakerForceOpen = new AtomicBoolean(false);
    private  AtomicBoolean circuitBreakerForceClosed = new AtomicBoolean(false);
}
