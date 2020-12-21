package com.github.vladimir_bukhtoyarov.token_bucket_basics_demo._4_lock_free_token_bucket;

import com.github.vladimir_bukhtoyarov.token_bucket_basics_demo.RateLimiter;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeTokenBucket implements RateLimiter {

    private final long capacity;
    private final long nanosToGenerateToken;
    private final AtomicReference<State> stateReference;

    private static final class State {

        private long availableTokens;
        private long lastRefillNanotime;

    }

    public LockFreeTokenBucket(long permits, Duration period) {
        this.nanosToGenerateToken = period.toNanos() / permits;
        this.capacity = permits;

        State initialState = new State();
        initialState.lastRefillNanotime = System.nanoTime();
        initialState.availableTokens = permits;

        this.stateReference = new AtomicReference<>(initialState);
    }

    @Override
    public boolean tryAcquire(int permits) {
        State newState = new State();
        while (true) {
            long now = System.nanoTime();
            State previousState = stateReference.get();
            newState.availableTokens = previousState.availableTokens;
            newState.lastRefillNanotime = previousState.lastRefillNanotime;
            refill(newState, now);
            if (newState.availableTokens < permits) {
                return false;
            }
            newState.availableTokens -= permits;
            if (stateReference.compareAndSet(previousState, newState)) {
                return true;
            }
        }
    }

    private void refill(State state, long now) {
        long nanosSinceLastRefill = now - state.lastRefillNanotime;
        if (nanosSinceLastRefill <= nanosToGenerateToken) {
            return;
        }
        long tokensSinceLastRefill = nanosSinceLastRefill / nanosToGenerateToken;
        state.availableTokens = Math.min(capacity, state.availableTokens + tokensSinceLastRefill);
        state.lastRefillNanotime += tokensSinceLastRefill * nanosToGenerateToken;
    }

}
