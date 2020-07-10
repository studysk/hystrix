package com.suke.hystrix.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

public class HystrixRollingNumber {
    private final Integer timeInMilliseconds;
    private final Integer numberOfBuckets;
    private final Time time;
    private static final Time ACTUAL_TIME = new ActualTime();
    private final BucketCircularArray buckets;
    private final CumulativeSum cumulativeSum = new CumulativeSum();

    private static interface Time {
        public long getCurrentTimeInMillis();
    }
    private static class ActualTime implements Time {

        @Override
        public long getCurrentTimeInMillis() {
            return System.currentTimeMillis();
        }

    }
    public HystrixRollingNumber(Integer timeInMilliseconds, Integer numberOfBuckets) {
        this(ACTUAL_TIME, timeInMilliseconds, numberOfBuckets);
    }

    private HystrixRollingNumber(Time time, Integer timeInMilliseconds, Integer numberOfBuckets) {
        this.time = time;
        this.timeInMilliseconds = timeInMilliseconds;
        this.numberOfBuckets = numberOfBuckets;

        if (timeInMilliseconds % numberOfBuckets != 0) {
            throw new IllegalArgumentException("The timeInMilliseconds must divide equally into numberOfBuckets. For example 1000/10 is ok, 1000/11 is not.");
        }

        buckets = new BucketCircularArray(numberOfBuckets);
    }
    private static class Bucket {
        final long windowStart;
        final LongAdder[] adderForCounterType;
        final LongMaxUpdater[] updaterForCounterType;

        Bucket(long startTime) {
            this.windowStart = startTime;

            /*
             * We support both LongAdder and LongMaxUpdater in a bucket but don't want the memory allocation
             * of all types for each so we only allocate the objects if the HystrixRollingNumberEvent matches
             * the correct type - though we still have the allocation of empty arrays to the given length
             * as we want to keep using the type.ordinal() value for fast random access.
             */

            // initialize the array of LongAdders
            adderForCounterType = new LongAdder[HystrixRollingNumberEvent.values().length];
            for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
                if (type.isCounter()) {
                    adderForCounterType[type.ordinal()] = new LongAdder();
                }
            }

            updaterForCounterType = new LongMaxUpdater[HystrixRollingNumberEvent.values().length];
            for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
                if (type.isMaxUpdater()) {
                    updaterForCounterType[type.ordinal()] = new LongMaxUpdater();
                    // initialize to 0 otherwise it is Long.MIN_VALUE
                    updaterForCounterType[type.ordinal()].update(0);
                }
            }
        }

        long get(HystrixRollingNumberEvent type) {
            if (type.isCounter()) {
                return adderForCounterType[type.ordinal()].sum();
            }
            if (type.isMaxUpdater()) {
                return updaterForCounterType[type.ordinal()].max();
            }
            throw new IllegalStateException("Unknown type of event: " + type.name());
        }

        LongAdder getAdder(HystrixRollingNumberEvent type) {
            if (!type.isCounter()) {
                throw new IllegalStateException("Type is not a Counter: " + type.name());
            }
            return adderForCounterType[type.ordinal()];
        }

        LongMaxUpdater getMaxUpdater(HystrixRollingNumberEvent type) {
            if (!type.isMaxUpdater()) {
                throw new IllegalStateException("Type is not a MaxUpdater: " + type.name());
            }
            return updaterForCounterType[type.ordinal()];
        }

    }
    private class BucketCircularArray implements Iterable<Bucket> {
        private final AtomicReference<ListState> state;
        private final int dataLength; // we don't resize, we always stay the same, so remember this
        private final int numBuckets;

        /**
         * Immutable object that is atomically set every time the state of the BucketCircularArray changes
         * <p>
         * This handles the compound operations
         */
        private class ListState {
            /*
             * this is an AtomicReferenceArray and not a normal Array because we're copying the reference
             * between ListState objects and multiple threads could maintain references across these
             * compound operations so I want the visibility/concurrency guarantees
             */
            private final AtomicReferenceArray<Bucket> data;
            private final int size;
            private final int tail;
            private final int head;

            private ListState(AtomicReferenceArray<Bucket> data, int head, int tail) {
                this.head = head;
                this.tail = tail;
                if (head == 0 && tail == 0) {
                    size = 0;
                } else {
                    this.size = (tail + dataLength - head) % dataLength;
                }
                this.data = data;
            }

            public Bucket tail() {
                if (size == 0) {
                    return null;
                } else {
                    // we want to get the last item, so size()-1
                    return data.get(convert(size - 1));
                }
            }

            private Bucket[] getArray() {
                /*
                 * this isn't technically thread-safe since it requires multiple reads on something that can change
                 * but since we never clear the data directly, only increment/decrement head/tail we would never get a NULL
                 * just potentially return stale data which we are okay with doing
                 */
                ArrayList<Bucket> array = new ArrayList<Bucket>();
                for (int i = 0; i < size; i++) {
                    array.add(data.get(convert(i)));
                }
                return array.toArray(new Bucket[array.size()]);
            }

            private ListState incrementTail() {
                /* if incrementing results in growing larger than 'length' which is the max we should be at, then also increment head (equivalent of removeFirst but done atomically) */
                if (size == numBuckets) {
                    // increment tail and head
                    return new ListState(data, (head + 1) % dataLength, (tail + 1) % dataLength);
                } else {
                    // increment only tail
                    return new ListState(data, head, (tail + 1) % dataLength);
                }
            }

            public ListState clear() {
                return new ListState(new AtomicReferenceArray<Bucket>(dataLength), 0, 0);
            }

            public ListState addBucket(Bucket b) {
                /*
                 * We could in theory have 2 threads addBucket concurrently and this compound operation would interleave.
                 * <p>
                 * This should NOT happen since getCurrentBucket is supposed to be executed by a single thread.
                 * <p>
                 * If it does happen, it's not a huge deal as incrementTail() will be protected by compareAndSet and one of the two addBucket calls will succeed with one of the Buckets.
                 * <p>
                 * In either case, a single Bucket will be returned as "last" and data loss should not occur and everything keeps in sync for head/tail.
                 * <p>
                 * Also, it's fine to set it before incrementTail because nothing else should be referencing that index position until incrementTail occurs.
                 */
                data.set(tail, b);
                return incrementTail();
            }

            // The convert() method takes a logical index (as if head was
            // always 0) and calculates the index within elementData
            private int convert(int index) {
                return (index + head) % dataLength;
            }
        }

        BucketCircularArray(int size) {
            AtomicReferenceArray<Bucket> _buckets = new AtomicReferenceArray<Bucket>(size + 1); // + 1 as extra room for the add/remove;
            state = new AtomicReference<ListState>(new ListState(_buckets, 0, 0));
            dataLength = _buckets.length();
            numBuckets = size;
        }

        public void clear() {
            while (true) {
                /*
                 * it should be very hard to not succeed the first pass thru since this is typically is only called from
                 * a single thread protected by a tryLock, but there is at least 1 other place (at time of writing this comment)
                 * where reset can be called from (CircuitBreaker.markSuccess after circuit was tripped) so it can
                 * in an edge-case conflict.
                 *
                 * Instead of trying to determine if someone already successfully called clear() and we should skip
                 * we will have both calls reset the circuit, even if that means losing data added in between the two
                 * depending on thread scheduling.
                 *
                 * The rare scenario in which that would occur, we'll accept the possible data loss while clearing it
                 * since the code has stated its desire to clear() anyways.
                 */
                ListState current = state.get();
                ListState newState = current.clear();
                if (state.compareAndSet(current, newState)) {
                    return;
                }
            }
        }

        /**
         * Returns an iterator on a copy of the internal array so that the iterator won't fail by buckets being added/removed concurrently.
         */
        public Iterator<Bucket> iterator() {
            return Collections.unmodifiableList(Arrays.asList(getArray())).iterator();
        }

        public void addLast(Bucket o) {
            ListState currentState = state.get();
            // create new version of state (what we want it to become)
            ListState newState = currentState.addBucket(o);

            /*
             * use compareAndSet to set in case multiple threads are attempting (which shouldn't be the case because since addLast will ONLY be called by a single thread at a time due to protection
             * provided in <code>getCurrentBucket</code>)
             */
            if (state.compareAndSet(currentState, newState)) {
                // we succeeded
                return;
            } else {
                // we failed, someone else was adding or removing
                // instead of trying again and risking multiple addLast concurrently (which shouldn't be the case)
                // we'll just return and let the other thread 'win' and if the timing is off the next call to getCurrentBucket will fix things
                return;
            }
        }

        public Bucket getLast() {
            return peekLast();
        }

        public int size() {
            // the size can also be worked out each time as:
            // return (tail + data.length() - head) % data.length();
            return state.get().size;
        }

        public Bucket peekLast() {
            return state.get().tail();
        }

        private Bucket[] getArray() {
            return state.get().getArray();
        }

    }
    public long getRollingSum(HystrixRollingNumberEvent type) {
        Bucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return 0;

        long sum = 0;
        for (Bucket b : buckets) {
            sum += b.getAdder(type).sum();
        }
        return sum;
    }
    private ReentrantLock newBucketLock = new ReentrantLock();
    private Bucket getCurrentBucket() {
        long currentTime = time.getCurrentTimeInMillis();

        /* a shortcut to try and get the most common result of immediately finding the current bucket */

        /**
         * Retrieve the latest bucket if the given time is BEFORE the end of the bucket window, otherwise it returns NULL.
         *
         * NOTE: This is thread-safe because it's accessing 'buckets' which is a LinkedBlockingDeque
         */
        Bucket currentBucket = buckets.peekLast();
        if (currentBucket != null && currentTime < currentBucket.windowStart + getBucketSizeInMilliseconds()) {
            // if we're within the bucket 'window of time' return the current one
            // NOTE: We do not worry if we are BEFORE the window in a weird case of where thread scheduling causes that to occur,
            // we'll just use the latest as long as we're not AFTER the window
            return currentBucket;
        }

        /* if we didn't find the current bucket above, then we have to create one */

        /**
         * The following needs to be synchronized/locked even with a synchronized/thread-safe data structure such as LinkedBlockingDeque because
         * the logic involves multiple steps to check existence, create an object then insert the object. The 'check' or 'insertion' themselves
         * are thread-safe by themselves but not the aggregate algorithm, thus we put this entire block of logic inside synchronized.
         *
         * I am using a tryLock if/then (http://download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/Lock.html#tryLock())
         * so that a single thread will get the lock and as soon as one thread gets the lock all others will go the 'else' block
         * and just return the currentBucket until the newBucket is created. This should allow the throughput to be far higher
         * and only slow down 1 thread instead of blocking all of them in each cycle of creating a new bucket based on some testing
         * (and it makes sense that it should as well).
         *
         * This means the timing won't be exact to the millisecond as to what data ends up in a bucket, but that's acceptable.
         * It's not critical to have exact precision to the millisecond, as long as it's rolling, if we can instead reduce the impact synchronization.
         *
         * More importantly though it means that the 'if' block within the lock needs to be careful about what it changes that can still
         * be accessed concurrently in the 'else' block since we're not completely synchronizing access.
         *
         * For example, we can't have a multi-step process to add a bucket, remove a bucket, then update the sum since the 'else' block of code
         * can retrieve the sum while this is all happening. The trade-off is that we don't maintain the rolling sum and let readers just iterate
         * bucket to calculate the sum themselves. This is an example of favoring write-performance instead of read-performance and how the tryLock
         * versus a synchronized block needs to be accommodated.
         */
        if (newBucketLock.tryLock()) {
            try {
                if (buckets.peekLast() == null) {
                    // the list is empty so create the first bucket
                    Bucket newBucket = new Bucket(currentTime);
                    buckets.addLast(newBucket);
                    return newBucket;
                } else {
                    // We go into a loop so that it will create as many buckets as needed to catch up to the current time
                    // as we want the buckets complete even if we don't have transactions during a period of time.
                    for (int i = 0; i < numberOfBuckets; i++) {
                        // we have at least 1 bucket so retrieve it
                        Bucket lastBucket = buckets.peekLast();
                        if (currentTime < lastBucket.windowStart + getBucketSizeInMilliseconds()) {
                            // if we're within the bucket 'window of time' return the current one
                            // NOTE: We do not worry if we are BEFORE the window in a weird case of where thread scheduling causes that to occur,
                            // we'll just use the latest as long as we're not AFTER the window
                            return lastBucket;
                        } else if (currentTime - (lastBucket.windowStart + getBucketSizeInMilliseconds()) > timeInMilliseconds) {
                            // the time passed is greater than the entire rolling counter so we want to clear it all and start from scratch
                            reset();
                            // recursively call getCurrentBucket which will create a new bucket and return it
                            return getCurrentBucket();
                        } else { // we're past the window so we need to create a new bucket
                            // create a new bucket and add it as the new 'last'
                            buckets.addLast(new Bucket(lastBucket.windowStart + getBucketSizeInMilliseconds()));
                            // add the lastBucket values to the cumulativeSum
                            cumulativeSum.addBucket(lastBucket);
                        }
                    }
                    // we have finished the for-loop and created all of the buckets, so return the lastBucket now
                    return buckets.peekLast();
                }
            } finally {
                newBucketLock.unlock();
            }
        } else {
            currentBucket = buckets.peekLast();
            if (currentBucket != null) {
                // we didn't get the lock so just return the latest bucket while another thread creates the next one
                return currentBucket;
            } else {
                // the rare scenario where multiple threads raced to create the very first bucket
                // wait slightly and then use recursion while the other thread finishes creating a bucket
                try {
                    Thread.sleep(5);
                } catch (Exception e) {
                    // ignore
                }
                return getCurrentBucket();
            }
        }
    }
    private int getBucketSizeInMilliseconds() {
        return timeInMilliseconds / numberOfBuckets;
    }
    private static class CumulativeSum {
        final LongAdder[] adderForCounterType;
        final LongMaxUpdater[] updaterForCounterType;

        CumulativeSum() {

            /*
             * We support both LongAdder and LongMaxUpdater in a bucket but don't want the memory allocation
             * of all types for each so we only allocate the objects if the HystrixRollingNumberEvent matches
             * the correct type - though we still have the allocation of empty arrays to the given length
             * as we want to keep using the type.ordinal() value for fast random access.
             */

            // initialize the array of LongAdders
            adderForCounterType = new LongAdder[HystrixRollingNumberEvent.values().length];
            for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
                if (type.isCounter()) {
                    adderForCounterType[type.ordinal()] = new LongAdder();
                }
            }

            updaterForCounterType = new LongMaxUpdater[HystrixRollingNumberEvent.values().length];
            for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
                if (type.isMaxUpdater()) {
                    updaterForCounterType[type.ordinal()] = new LongMaxUpdater();
                    // initialize to 0 otherwise it is Long.MIN_VALUE
                    updaterForCounterType[type.ordinal()].update(0);
                }
            }
        }

        public void addBucket(Bucket lastBucket) {
            for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
                if (type.isCounter()) {
                    getAdder(type).add(lastBucket.getAdder(type).sum());
                }
                if (type.isMaxUpdater()) {
                    getMaxUpdater(type).update(lastBucket.getMaxUpdater(type).max());
                }
            }
        }

        long get(HystrixRollingNumberEvent type) {
            if (type.isCounter()) {
                return adderForCounterType[type.ordinal()].sum();
            }
            if (type.isMaxUpdater()) {
                return updaterForCounterType[type.ordinal()].max();
            }
            throw new IllegalStateException("Unknown type of event: " + type.name());
        }

        LongAdder getAdder(HystrixRollingNumberEvent type) {
            if (!type.isCounter()) {
                throw new IllegalStateException("Type is not a Counter: " + type.name());
            }
            return adderForCounterType[type.ordinal()];
        }

        LongMaxUpdater getMaxUpdater(HystrixRollingNumberEvent type) {
            if (!type.isMaxUpdater()) {
                throw new IllegalStateException("Type is not a MaxUpdater: " + type.name());
            }
            return updaterForCounterType[type.ordinal()];
        }

    }
    public void reset() {
        // if we are resetting, that means the lastBucket won't have a chance to be captured in CumulativeSum, so let's do it here
        Bucket lastBucket = buckets.peekLast();
        if (lastBucket != null) {
            cumulativeSum.addBucket(lastBucket);
        }

        // clear buckets so we start over again
        buckets.clear();
    }
    public void increment(HystrixRollingNumberEvent type) {
        getCurrentBucket().getAdder(type).increment();
    }
}
