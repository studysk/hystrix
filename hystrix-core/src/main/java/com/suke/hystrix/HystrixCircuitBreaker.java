package com.suke.hystrix;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public interface HystrixCircuitBreaker {

    boolean allowRequest();

    static class HystrixCircuitBreakerImpl implements HystrixCircuitBreaker{
        private final HystrixCommandProperties properties;
        private final HystrixCommandMetrics metrics;
        private AtomicLong circuitOpenedOrLastTestedTime = new AtomicLong();

        private AtomicBoolean circuitOpen = new AtomicBoolean(false);

        protected HystrixCircuitBreakerImpl( HystrixCommandProperties properties,HystrixCommandMetrics metrics) {
            this.properties = properties;
            this.metrics = metrics;
        }
        @Override
        public boolean allowRequest() {
            //熔断器是否打开
            if(properties.circuitBreakerForceOpen.get()){
                return false;
            }
            //熔断器是否强制关闭
            if(properties.circuitBreakerForceClosed.get()){
                isOpen();
                return true;
            }
            return !isOpen() || allowSingleTest();
        }

        private boolean allowSingleTest() {
            return false;
        }
        @Override
        public void markSuccess() {
            if (circuitOpen.get()) {
                // If we have been 'open' and have a success then we want to close the circuit. This handles the 'singleTest' logic
                circuitOpen.set(false);
                // TODO how can we can do this without resetting the counts so we don't lose metrics of short-circuits etc?
                metrics.resetCounter();
            }
        }
        private boolean isOpen() {
            if (circuitOpen.get()) {
                // if we're open we immediately return true and don't bother attempting to 'close' ourself as that is left to allowSingleTest and a subsequent successful test to close
                return true;
            }
            // we're closed, so let's see if errors have made us so we should trip the circuit open

            // check if we are past the statisticalWindowVolumeThreshold
            if (metrics.getHealthCounts().getTotalRequests() < properties.circuitBreakerRequestVolumeThreshold) {
                // we are not past the minimum volume threshold for the statisticalWindow so we'll return false immediately and not calculate anything
                return false;
            }
            if (metrics.getHealthCounts().getErrorPercentage() < properties.circuitBreakerErrorThresholdPercentage) {
                return false;
            } else {
                // our failure rate is too high, trip the circuit
                if (circuitOpen.compareAndSet(false, true)) {
                    // if the previousValue was false then we want to set the currentTime
                    // How could previousValue be true? If another thread was going through this code at the same time a race-condition could have
                    // caused another thread to set it to true already even though we were in the process of doing the same
                    circuitOpenedOrLastTestedTime.set(System.currentTimeMillis());
                }
                return true;
            }
        }
    }
    void markSuccess();
    public static class Factory {
        // String is HystrixCommandKey.name() (we can't use HystrixCommandKey directly as we can't guarantee it implements hashcode/equals correctly)
        private static ConcurrentHashMap<String, HystrixCircuitBreaker> circuitBreakersByCommand = new ConcurrentHashMap<String, HystrixCircuitBreaker>();

        /**
         * Get the {@link HystrixCircuitBreaker} instance for a given {@link HystrixCommandKey}.
         * <p>
         * This is thread-safe and ensures only 1 {@link HystrixCircuitBreaker} per {@link HystrixCommandKey}.
         *
         * @param key
         *            {@link HystrixCommandKey} of {@link HystrixCommand} instance requesting the {@link HystrixCircuitBreaker}
         * @param group
         *            Pass-thru to {@link HystrixCircuitBreaker}
         * @param properties
         *            Pass-thru to {@link HystrixCircuitBreaker}
         * @param metrics
         *            Pass-thru to {@link HystrixCircuitBreaker}
         * @return {@link HystrixCircuitBreaker} for {@link HystrixCommandKey}
         */
        public static HystrixCircuitBreaker getInstance(HystrixCommandKey key, HystrixCommandGroupKey group, HystrixCommandProperties properties, HystrixCommandMetrics metrics) {
            // this should find it for all but the first time
            HystrixCircuitBreaker previouslyCached = circuitBreakersByCommand.get(key.name());
            if (previouslyCached != null) {
                return previouslyCached;
            }

            // if we get here this is the first time so we need to initialize

            // Create and add to the map ... use putIfAbsent to atomically handle the possible race-condition of
            // 2 threads hitting this point at the same time and let ConcurrentHashMap provide us our thread-safety
            // If 2 threads hit here only one will get added and the other will get a non-null response instead.
            HystrixCircuitBreaker cbForCommand = circuitBreakersByCommand.putIfAbsent(key.name(), new HystrixCircuitBreakerImpl( properties, metrics));
            if (cbForCommand == null) {
                // this means the putIfAbsent step just created a new one so let's retrieve and return it
                return circuitBreakersByCommand.get(key.name());
            } else {
                // this means a race occurred and while attempting to 'put' another one got there before
                // and we instead retrieved it and will now return it
                return cbForCommand;
            }
        }
    }
    static class NoOpCircuitBreaker implements HystrixCircuitBreaker {

        @Override
        public boolean allowRequest() {
            return true;
        }

        public boolean isOpen() {
            return false;
        }

        @Override
        public void markSuccess() {

        }

    }
}
