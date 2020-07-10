package com.suke.hystrix;

import com.suke.hystrix.exception.HystrixBadRequestException;
import com.suke.hystrix.exception.HystrixRuntimeException;
import com.suke.hystrix.properties.HystrixPropertiesFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class HystrixCommand<R> implements HystrixExecutable{
    private  HystrixCircuitBreaker circuitBreaker;
    private  HystrixCommandMetrics metrics;
    /* FALLBACK Semaphore */
    private TryableSemaphore fallbackSemaphoreOverride;
    private static final ConcurrentHashMap<String, TryableSemaphore> fallbackSemaphorePerCircuit = new ConcurrentHashMap<String, TryableSemaphore>();
    private AtomicLong invocationStartTime = new AtomicLong(-1);
    private  HystrixCommandKey commandKey;
    private  HystrixCommandProperties properties;
    private volatile ExecutionResult executionResult = ExecutionResult.EMPTY;
    private final AtomicBoolean isExecutionComplete = new AtomicBoolean(false);
    private TryableSemaphore executionSemaphoreOverride;
    private final AtomicBoolean isCommandTimedOut = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, TryableSemaphore> executionSemaphorePerCircuit = new ConcurrentHashMap<String, TryableSemaphore>();
    private  HystrixCommandGroupKey commandGroup;
    private static ConcurrentHashMap<Class<? extends HystrixCommand>, String> defaultNameCache = new ConcurrentHashMap<Class<? extends HystrixCommand>, String>();
    protected HystrixCommand(HystrixCommandGroupKey group) {
        // use 'null' to specify use the default
        this(new Setter(group));
    }
    protected HystrixCommand(Setter setter) {
        // use 'null' to specify use the default
        this(setter.groupKey, setter.commandKey, null, null,null,null);
    }
    private static String getDefaultNameFromClass(@SuppressWarnings("rawtypes") Class<? extends HystrixCommand> cls) {
        String fromCache = defaultNameCache.get(cls);
        if (fromCache != null) {
            return fromCache;
        }
        // generate the default
        // default HystrixCommandKey to use if the method is not overridden
        String name = cls.getSimpleName();
        if (name.equals("")) {
            // we don't have a SimpleName (anonymous inner class) so use the full class name
            name = cls.getName();
            name = name.substring(name.lastIndexOf('.') + 1, name.length());
        }
        defaultNameCache.put(cls, name);
        return name;
    }
    private HystrixCommand(HystrixCommandGroupKey group, HystrixCommandKey key, HystrixCircuitBreaker circuitBreaker,
                            HystrixCommandMetrics metrics,  TryableSemaphore fallbackSemaphore, TryableSemaphore executionSemaphore) {
        /*
         * CommandGroup initialization
         */
        if (group == null) {
            throw new IllegalStateException("HystrixCommandGroup can not be NULL");
        } else {
            this.commandGroup = group;
        }

        /*
         * CommandKey initialization
         */
        if (key == null || key.name().trim().equals("")) {
            final String keyName = getDefaultNameFromClass(getClass());
            this.commandKey = HystrixCommandKey.Factory.asKey(keyName);
        } else {
            this.commandKey = key;
        }

        /*
         * Properties initialization
         */
        this.properties = HystrixPropertiesFactory.getCommandProperties();


        /*
         * Metrics initialization
         */
        if (metrics == null) {
            this.metrics = HystrixCommandMetrics.getInstance(this.commandKey, this.commandGroup,  this.properties);
        } else {
            this.metrics = metrics;
        }

        /*
         * CircuitBreaker initialization
         */
        if (this.properties.circuitBreakerEnabled) {
            if (circuitBreaker == null) {
                // get the default implementation of HystrixCircuitBreaker
                this.circuitBreaker = HystrixCircuitBreaker.Factory.getInstance(this.commandKey, this.commandGroup, this.properties, this.metrics);
            } else {
                this.circuitBreaker = circuitBreaker;
            }
        } else {
            this.circuitBreaker = new HystrixCircuitBreaker.NoOpCircuitBreaker();
        }

        /* fallback semaphore override if applicable */
        this.fallbackSemaphoreOverride = fallbackSemaphore;

        /* execution semaphore override if applicable */
        this.executionSemaphoreOverride = executionSemaphore;
    }
    public static class Setter {

        private final HystrixCommandGroupKey groupKey;
        private HystrixCommandKey commandKey;
//        private HystrixCommandProperties.Setter commandPropertiesDefaults;

        /**
         * Setter factory method containing required values.
         * <p>
         * All optional arguments can be set via the chained methods.
         *
         * @param groupKey
         *            {@link HystrixCommandGroupKey} used to group together multiple {@link HystrixCommand} objects.
         *            <p>
         *            The {@link HystrixCommandGroupKey} is used to represent a common relationship between commands. For example, a library or team name, the system all related commands interace
         *            with,
         *            common business purpose etc.
         */
        private Setter(HystrixCommandGroupKey groupKey) {
            this.groupKey = groupKey;
        }

        /**
         * Setter factory method with required values.
         * <p>
         * All optional arguments can be set via the chained methods.
         *
         * @param groupKey
         *            {@link HystrixCommandGroupKey} used to group together multiple {@link HystrixCommand} objects.
         *            <p>
         *            The {@link HystrixCommandGroupKey} is used to represent a common relationship between commands. For example, a library or team name, the system all related commands interace
         *            with,
         *            common business purpose etc.
         */
        public static Setter withGroupKey(HystrixCommandGroupKey groupKey) {
            return new Setter(groupKey);
        }

        /**
         * @param commandKey
         *            {@link HystrixCommandKey} used to identify a {@link HystrixCommand} instance for statistics, circuit-breaker, properties, etc.
         *            <p>
         *            By default this will be derived from the instance class name.
         *            <p>
         *            NOTE: Every unique {@link HystrixCommandKey} will result in new instances of {@link HystrixCircuitBreaker}, {@link HystrixCommandMetrics} and {@link HystrixCommandProperties}.
         *            Thus,
         *            the number of variants should be kept to a finite and reasonable number to avoid high-memory usage or memory leacks.
         *            <p>
         *            Hundreds of keys is fine, tens of thousands is probably not.
         * @return Setter for fluent interface via method chaining
         */
        public Setter andCommandKey(HystrixCommandKey commandKey) {
            this.commandKey = commandKey;
            return this;
        }


    }

    @Override
    public R execute() {
        try{
            if (!invocationStartTime.compareAndSet(-1, System.currentTimeMillis())) {
                throw new IllegalStateException("This instance can only be executed once. Please instantiate a new instance.");
            }
            try{
                if(!circuitBreaker.allowRequest()){
                    metrics.markShortCircuited();
                    // short-circuit and go directly to fallback
                    return getFallbackOrThrowException(HystrixEventType.SHORT_CIRCUITED, HystrixRuntimeException.FailureType.SHORTCIRCUIT, "short-circuited");
                }
                try {
                    R r = executeWithSemaphore();
                    return r;
                } catch (RuntimeException e) {
                    // count that we're throwing an exception and rethrow
                    metrics.markExceptionThrown();
                    throw e;
                }
            }catch (Throwable e){
                throw new HystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION, this.getClass(), null, e, null);
            }
        }finally {
            //to-do log
        }

    }
    private R getFallbackOrThrowException(HystrixEventType eventType, HystrixRuntimeException.FailureType failureType, String message) {
        return getFallbackOrThrowException(eventType, failureType, message, null);
    }
    private static class ExecutionResult {
        private final List<HystrixEventType> events;
        private final int executionTime;

        private ExecutionResult(HystrixEventType... events) {
            this(Arrays.asList(events), -1);
        }

        public ExecutionResult setExecutionTime(int executionTime) {
            return new ExecutionResult(events, executionTime);
        }

        private ExecutionResult(List<HystrixEventType> events, int executionTime) {
            // we are safe assigning the List reference instead of deep-copying
            // because we control the original list in 'newEvent'
            this.events = events;
            this.executionTime = executionTime;
        }

        // we can return a static version since it's immutable
        private static ExecutionResult EMPTY = new ExecutionResult(new HystrixEventType[0]);

        /**
         * Creates a new ExecutionResult by adding the defined 'events' to the ones on the current instance.
         *
         * @param events
         * @return
         */
        public ExecutionResult addEvents(HystrixEventType... events) {
            ArrayList<HystrixEventType> newEvents = new ArrayList<HystrixEventType>();
            newEvents.addAll(this.events);
            for (HystrixEventType e : events) {
                newEvents.add(e);
            }
            return new ExecutionResult(Collections.unmodifiableList(newEvents), executionTime);
        }

    }
    private R getFallbackOrThrowException(HystrixEventType eventType, HystrixRuntimeException.FailureType failureType, String message, Throwable e) {
        try {
            // retrieve the fallback
            R fallback = getFallbackWithProtection();
            // mark fallback on counter
            metrics.markFallbackSuccess();
            // record the executionResult
            executionResult = executionResult.addEvents(eventType, HystrixEventType.FALLBACK_SUCCESS);
            return fallback;
        } catch (UnsupportedOperationException fe) {
//            logger.debug("No fallback for HystrixCommand. ", fe); // debug only since we're throwing the exception and someone higher will do something with it
            // record the executionResult
            executionResult = executionResult.addEvents(eventType);
            throw new HystrixRuntimeException(failureType, this.getClass(), getLogMessagePrefix() + " " + message + " and no fallback available.", e, fe);
        } catch (Throwable fe) {
//            logger.error("Error retrieving fallback for HystrixCommand. ", fe);
            metrics.markFallbackFailure();
            // record the executionResult
            executionResult = executionResult.addEvents(eventType, HystrixEventType.FALLBACK_FAILURE);
            throw new HystrixRuntimeException(failureType, this.getClass(), getLogMessagePrefix() + " " + message + " and failed retrieving fallback.", e, fe);
        } finally {
            // record that we're completed (to handle non-successful events we do it here as well as at the end of executeCommand
            isExecutionComplete.set(true);
        }
    }
    private R getFallbackWithProtection() {
        TryableSemaphore fallbackSemaphore = getFallbackSemaphore();
        // acquire a permit
        if (fallbackSemaphore.tryAcquire()) {
            try {
                return getFallback();
            } finally {
                fallbackSemaphore.release();
            }
        } else {
            metrics.markFallbackRejection();
//            logger.debug("HystrixCommand Fallback Rejection."); // debug only since we're throwing the exception and someone higher will do something with it
            // if we couldn't acquire a permit, we "fail fast" by throwing an exception
            throw new HystrixRuntimeException(HystrixRuntimeException.FailureType.REJECTED_SEMAPHORE_FALLBACK, this.getClass(), getLogMessagePrefix() + " fallback execution rejected.", null, null);
        }
    }
    private String getLogMessagePrefix() {
        return getCommandKey().name();
    }
    public final HystrixCommandKey getCommandKey() {
        return commandKey;
    }
    protected R getFallback() {
        throw new UnsupportedOperationException("No fallback available.");
    }
    private TryableSemaphore getFallbackSemaphore() {
        if (fallbackSemaphoreOverride == null) {
            TryableSemaphore _s = fallbackSemaphorePerCircuit.get(commandKey.name());
            if (_s == null) {
                // we didn't find one cache so setup
                fallbackSemaphorePerCircuit.putIfAbsent(commandKey.name(), new TryableSemaphore(properties.fallbackIsolationSemaphoreMaxConcurrentRequests));
                // assign whatever got set (this or another thread)
                return fallbackSemaphorePerCircuit.get(commandKey.name());
            } else {
                return _s;
            }
        } else {
            return fallbackSemaphoreOverride;
        }
    }
    private static class TryableSemaphore {
        private final Integer numberOfPermits;
        private static AtomicInteger count = new AtomicInteger(0);

        public TryableSemaphore(Integer numberOfPermits) {
            this.numberOfPermits = numberOfPermits;
        }

        /**
         * Use like this:
         * <p>
         *
         * <pre>
         * if (s.tryAcquire()) {
         * try {
         * // do work that is protected by 's'
         * } finally {
         * s.release();
         * }
         * }
         * </pre>
         *
         * @return boolean
         */
        public boolean tryAcquire() {
            int currentCount = count.incrementAndGet();
            if (currentCount > numberOfPermits) {
                count.decrementAndGet();
                return false;
            } else {
                return true;
            }
        }

        /**
         * ONLY call release if tryAcquire returned true.
         * <p>
         *
         * <pre>
         * if (s.tryAcquire()) {
         * try {
         * // do work that is protected by 's'
         * } finally {
         * s.release();
         * }
         * }
         * </pre>
         */
        public void release() {
            count.decrementAndGet();
        }

        public int getNumberOfPermitsUsed() {
            return count.get();
        }

        public int getNumberOfPermits() {
            return numberOfPermits;
        }

    }
    private  R executeWithSemaphore(){
        TryableSemaphore executionSemaphore = getExecutionSemaphore();
        // acquire a permit
        if (executionSemaphore.tryAcquire()) {
            try {
                // allow tracking how many concurrent threads are in here the same way we do with threadpool
                metrics.markExecutionSemaphoreUsedPermitsCount(executionSemaphore.getNumberOfPermitsUsed());

                // we want to run it synchronously
                R response = executeCommand();
                // put in cache
//                if (isRequestCachingEnabled()) {
//                    requestCache.putIfAbsent(getCacheKey(), asFutureForCache(response));
//                }
                /*
                 * We don't bother looking for whether someone else also put it in the cache since we've already executed and received a response.
                 * In this path we are synchronous so don't have the option of queuing a Future.
                 */
                return response;
            } finally {
                executionSemaphore.release();
            }
        } else {
            // mark on counter
            metrics.markSemaphoreRejection();
//            logger.debug("HystrixCommand Execution Rejection by Semaphore"); // debug only since we're throwing the exception and someone higher will do something with it
            return getFallbackOrThrowException(HystrixEventType.SEMAPHORE_REJECTED, HystrixRuntimeException.FailureType.REJECTED_SEMAPHORE_EXECUTION, "could not acquire a semaphore for execution");
        }
    }
    protected abstract R run();
    private R executeCommand() {
        /**
         * NOTE: Be very careful about what goes in this method. It gets invoked within another thread in most circumstances.
         *
         * The modifications of booleans 'isResponseFromFallback' etc are going across thread-boundaries thus those
         * variables MUST be volatile otherwise they are not guaranteed to be seen by the user thread when the executing thread modifies them.
         */

        /* capture start time for logging */
        long startTime = System.currentTimeMillis();
        try {
            R response = run();
            long duration = System.currentTimeMillis() - startTime;
            metrics.addCommandExecutionTime(duration);

            if (isCommandTimedOut.get()) {
                // the command timed out in the wrapping thread so we will return immediately
                // and not increment any of the counters below or other such logic
                return null;
            } else {
                // report success
                executionResult = executionResult.addEvents(HystrixEventType.SUCCESS);
                metrics.markSuccess(duration);
                circuitBreaker.markSuccess();
//                eventNotifier.markCommandExecution(getCommandKey(), properties.executionIsolationStrategy().get(), (int) duration, executionResult.events);
                return response;
            }
        } catch (HystrixBadRequestException e) {
            /*
             * HystrixBadRequestException is treated differently and allowed to propagate without any stats tracking or fallback logic
             */
            throw e;
        } catch (Throwable e) {
            if (isCommandTimedOut.get()) {
                // http://jira/browse/API-4905 HystrixCommand: Error/Timeout Double-count if both occur
                // this means we have already timed out then we don't count this error stat and we just return
                // as this means the user-thread has already returned, we've already done fallback logic
                // and we've already counted the timeout stat
//                logger.error("Error executing HystrixCommand [TimedOut]", e);
                return null;
            } else {
//                logger.error("Error executing HystrixCommand", e);
            }
            // report failure
            metrics.markFailure(System.currentTimeMillis() - startTime);
            return getFallbackOrThrowException(HystrixEventType.FAILURE, HystrixRuntimeException.FailureType.COMMAND_EXCEPTION, "failed", e);
        } finally {
            /*
             * we record the executionTime for command execution
             * if the command is never executed (rejected, short-circuited, etc) then it will be left unset
             * for this metric we include failures and successes as we use it for per-request profiling and debugging
             * whereas 'metrics.addCommandExecutionTime(duration)' is used by stats across many requests
             */
            executionResult = executionResult.setExecutionTime((int) (System.currentTimeMillis() - startTime));

            // record that we're completed
            isExecutionComplete.set(true);
        }
    }
    private TryableSemaphore getExecutionSemaphore() {
        if (executionSemaphoreOverride == null) {
            TryableSemaphore _s = executionSemaphorePerCircuit.get(commandKey.name());
            if (_s == null) {
                // we didn't find one cache so setup
                executionSemaphorePerCircuit.putIfAbsent(commandKey.name(), new TryableSemaphore(properties.executionIsolationSemaphoreMaxConcurrentRequests));
                // assign whatever got set (this or another thread)
                return executionSemaphorePerCircuit.get(commandKey.name());
            } else {
                return _s;
            }
        } else {
            return executionSemaphoreOverride;
        }
    }
}
