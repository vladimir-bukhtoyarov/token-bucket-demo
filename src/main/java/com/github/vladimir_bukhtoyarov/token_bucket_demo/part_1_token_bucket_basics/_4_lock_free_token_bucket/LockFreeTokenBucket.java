package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_1_token_bucket_basics._4_lock_free_token_bucket;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeTokenBucket {

    private final long capacity;
    private final long nanosToGenerateToken;
    private final AtomicReference<State> stateReference;

    private static final class State {

        private long availableTokens;
        private long lastRefillNanotime;

    }

    public LockFreeTokenBucket(long capacity, Duration periodToGenerateOneToken) {
        this.nanosToGenerateToken = periodToGenerateOneToken.toNanos();
        this.capacity = capacity;

        State initialState = new State();
        initialState.lastRefillNanotime = System.nanoTime();
        initialState.availableTokens = capacity;

        this.stateReference = new AtomicReference<>(initialState);
    }

    public boolean tryConsume(int numberTokens) {
        State newState = new State();
        while (true) {
            State previousState = stateReference.get();
            newState.availableTokens = previousState.availableTokens;
            newState.lastRefillNanotime = previousState.lastRefillNanotime;

            refill(newState);

            if (newState.availableTokens < numberTokens) {
                return false;
            }
            newState.availableTokens -= numberTokens;
            if (stateReference.compareAndSet(previousState, newState)) {
                return true;
            }
        }
    }

    private void refill(State state) {
        long now = System.nanoTime();
        long nanosSinceLastRefill = now - state.lastRefillNanotime;
        if (nanosSinceLastRefill <= nanosToGenerateToken) {
            return;
        }
        long tokensSinceLastRefill = nanosSinceLastRefill / nanosToGenerateToken;
        state.availableTokens = Math.min(capacity, state.availableTokens + tokensSinceLastRefill);
        state.lastRefillNanotime += tokensSinceLastRefill * nanosToGenerateToken;
    }

}
