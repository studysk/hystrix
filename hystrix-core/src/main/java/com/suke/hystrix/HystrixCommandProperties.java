package com.suke.hystrix;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
public abstract class HystrixCommandProperties {
    public  AtomicBoolean circuitBreakerForceOpen = new AtomicBoolean(false);
    public  AtomicBoolean circuitBreakerForceClosed = new AtomicBoolean(false);
    public AtomicInteger metricsHealthSnapshotIntervalInMilliseconds = new AtomicInteger(500);
    public  Integer metricsRollingStatisticalWindowBuckets = 10;
    public Integer metricsRollingStatisticalWindowInMilliseconds = 10000;
    public Integer circuitBreakerRequestVolumeThreshold = 20;
    public Integer circuitBreakerErrorThresholdPercentage = 50;
    public Integer fallbackIsolationSemaphoreMaxConcurrentRequests = 10;
    public Integer executionIsolationSemaphoreMaxConcurrentRequests = 10;
    public boolean metricsRollingPercentileEnabled = true;
    public  Boolean circuitBreakerEnabled = true;


}
