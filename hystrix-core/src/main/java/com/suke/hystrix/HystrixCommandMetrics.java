package com.suke.hystrix;

import com.suke.hystrix.util.HystrixRollingNumber;
import com.suke.hystrix.util.HystrixRollingNumberEvent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HystrixCommandMetrics {

    private volatile AtomicLong lastHealthCountsSnapshot = new AtomicLong(System.currentTimeMillis());
    private final HystrixCommandProperties properties;
    private final HystrixRollingNumber counter;
    private volatile HealthCounts healthCountsSnapshot = null;
    private final AtomicInteger executionSemaphorePermitsInUse = new AtomicInteger();
    private  HystrixCommandKey key;
    private static final ConcurrentHashMap<String, HystrixCommandMetrics> metrics = new ConcurrentHashMap<String, HystrixCommandMetrics>();

    HystrixCommandMetrics(HystrixCommandKey key, HystrixCommandProperties properties) {
        this.key = key;
        this.properties = properties;
        this.counter = new HystrixRollingNumber(properties.metricsRollingStatisticalWindowInMilliseconds, properties.metricsRollingStatisticalWindowBuckets);
    }
    public HealthCounts getHealthCounts() {
        // we put an interval between snapshots so high-volume commands don't
        // spend too much unnecessary time calculating metrics in very small time periods
        long lastTime = lastHealthCountsSnapshot.get();
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime >= properties.metricsHealthSnapshotIntervalInMilliseconds.get() || healthCountsSnapshot == null) {
            if (lastHealthCountsSnapshot.compareAndSet(lastTime, currentTime)) {
                // our thread won setting the snapshot time so we will proceed with generating a new snapshot
                // losing threads will continue using the old snapshot
                long success = counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS);
                long failure = counter.getRollingSum(HystrixRollingNumberEvent.FAILURE); // fallbacks occur on this
                long timeout = counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT); // fallbacks occur on this
                long threadPoolRejected = counter.getRollingSum(HystrixRollingNumberEvent.THREAD_POOL_REJECTED); // fallbacks occur on this
                long semaphoreRejected = counter.getRollingSum(HystrixRollingNumberEvent.SEMAPHORE_REJECTED); // fallbacks occur on this
                long shortCircuited = counter.getRollingSum(HystrixRollingNumberEvent.SHORT_CIRCUITED); // fallbacks occur on this
                long totalCount = failure + success + timeout + threadPoolRejected + shortCircuited + semaphoreRejected;
                long errorCount = failure + timeout + threadPoolRejected + shortCircuited + semaphoreRejected;
                int errorPercentage = 0;

                if (totalCount > 0) {
                    errorPercentage = (int) ((double) errorCount / totalCount * 100);
                }

                healthCountsSnapshot = new HealthCounts(totalCount, errorCount, errorPercentage);
            }
        }
        return healthCountsSnapshot;
    }
    public static class HealthCounts {
        private final long totalCount;
        private final long errorCount;
        private final int errorPercentage;

        HealthCounts(long total, long error, int errorPercentage) {
            this.totalCount = total;
            this.errorCount = error;
            this.errorPercentage = errorPercentage;
        }

        public long getTotalRequests() {
            return totalCount;
        }

        public long getErrorCount() {
            return errorCount;
        }

        public int getErrorPercentage() {
            return errorPercentage;
        }
    }
    void markFallbackSuccess() {
        counter.increment(HystrixRollingNumberEvent.FALLBACK_SUCCESS);
    }
    public void markShortCircuited() {
        counter.increment(HystrixRollingNumberEvent.SHORT_CIRCUITED);
    }
    void markExceptionThrown() {
        counter.increment(HystrixRollingNumberEvent.EXCEPTION_THROWN);
    }
    void markFallbackRejection() {
        counter.increment(HystrixRollingNumberEvent.FALLBACK_REJECTION);
    }
    void markFallbackFailure() {
        counter.increment(HystrixRollingNumberEvent.FALLBACK_FAILURE);
    }
    void markExecutionSemaphoreUsedPermitsCount(int numberOfPermitsUsed) {
        executionSemaphorePermitsInUse.set(numberOfPermitsUsed);
    }
    void markSemaphoreRejection() {
        counter.increment(HystrixRollingNumberEvent.SEMAPHORE_REJECTED);
    }
    void addCommandExecutionTime(long duration) {
        if (properties.metricsRollingPercentileEnabled) {
//            percentileExecution.addValue((int) duration);
        }
    }
    void markSuccess(long duration) {
        counter.increment(HystrixRollingNumberEvent.SUCCESS);
    }
    void resetCounter() {
        counter.reset();
        // TODO can we do without this somehow?
    }
    void markFailure(long duration) {
        counter.increment(HystrixRollingNumberEvent.FAILURE);
    }
    HystrixCommandMetrics(HystrixCommandKey key, HystrixCommandGroupKey commandGroup,  HystrixCommandProperties properties) {
        this.key = key;
        this.properties = properties;
        this.counter = new HystrixRollingNumber(properties.metricsRollingStatisticalWindowInMilliseconds, properties.metricsRollingStatisticalWindowBuckets);
    }
    public static HystrixCommandMetrics getInstance(HystrixCommandKey key, HystrixCommandGroupKey commandGroup, HystrixCommandProperties properties) {
        // attempt to retrieve from cache first
        HystrixCommandMetrics commandMetrics = metrics.get(key.name());
        if (commandMetrics != null) {
            return commandMetrics;
        }
        // it doesn't exist so we need to create it
        commandMetrics = new HystrixCommandMetrics(key, commandGroup, properties);
        // attempt to store it (race other threads)
        HystrixCommandMetrics existing = metrics.putIfAbsent(key.name(), commandMetrics);
        if (existing == null) {
            // we won the thread-race to store the instance we created
            return commandMetrics;
        } else {
            // we lost so return 'existing' and let the one we created be garbage collected
            return existing;
        }
    }
}
