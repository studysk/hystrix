package com.suke.hystrix;

import java.util.concurrent.atomic.AtomicBoolean;

public interface HystrixCircuitBreaker {

    boolean allowRequest();

    static class HystrixCircuitBreakerImpl implements HystrixCircuitBreaker{
        private final HystrixCommandProperties properties;
        private AtomicBoolean circuitOpen = new AtomicBoolean(false);
        protected HystrixCircuitBreakerImpl( HystrixCommandProperties properties) {
            this.properties = properties;
        }
        public boolean allowRequest() {
            //熔断器是否打开
            if(properties.getCircuitBreakerForceOpen().get()){
                return false;
            }
            //熔断器是否强制关闭
            if(properties.getCircuitBreakerForceClosed().get()){
                return true;
            }
            return !isOpen() || allowSingleTest();
        }

        private boolean allowSingleTest() {
            return false;
        }

        private boolean isOpen() {
            if (circuitOpen.get()) {
                // if we're open we immediately return true and don't bother attempting to 'close' ourself as that is left to allowSingleTest and a subsequent successful test to close
                return true;
            }
            return false;
        }
    }
}
