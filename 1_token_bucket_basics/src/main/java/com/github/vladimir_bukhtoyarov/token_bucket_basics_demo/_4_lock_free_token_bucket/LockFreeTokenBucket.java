package com.github.vladimir_bukhtoyarov.token_bucket_basics_demo._4_lock_free_token_bucket;

import com.github.vladimir_bukhtoyarov.token_bucket_basics_demo.RateLimiter;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.nanoTime;











public class LockFreeTokenBucket implements RateLimiter {

    private final BucketParams params;
    private final AtomicReference<BucketState> stateReference;

    public LockFreeTokenBucket(long permits, Duration period) {
        this.params = new BucketParams(permits, period);
        this.stateReference = new AtomicReference<>(new BucketState(params, nanoTime()));
    }

    public boolean tryAcquire(int permits) {
        while (true) {
            long nanoTime = nanoTime();
            BucketState previousState = stateReference.get();
            BucketState newState = new BucketState(previousState);
            newState.refill(params, nanoTime);
            if (newState.availableTokens < permits) {
                return false;
            }
            newState.availableTokens -= permits;
            if (stateReference.compareAndSet(previousState, newState)) {
                return true;
            }
        }
    }
}





