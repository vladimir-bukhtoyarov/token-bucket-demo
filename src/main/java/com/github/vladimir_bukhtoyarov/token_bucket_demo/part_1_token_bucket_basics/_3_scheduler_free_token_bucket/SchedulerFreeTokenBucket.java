package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_1_token_bucket_basics._3_scheduler_free_token_bucket;

import java.time.Duration;

public class SchedulerFreeTokenBucket {

    private final long capacity;
    private final long nanosToGenerateToken;

    private long availableTokens;
    private long lastRefillNanotime;

    public SchedulerFreeTokenBucket(long capacity, Duration periodToGenerateOneToken) {
        this.nanosToGenerateToken = periodToGenerateOneToken.toNanos();
        this.lastRefillNanotime = System.nanoTime();

        this.capacity = capacity;
        this.availableTokens = capacity;
    }

    synchronized public boolean tryConsume(int numberTokens) {
        refill();
        if (availableTokens < numberTokens) {
            return false;
        } else {
            availableTokens -= numberTokens;
            return true;
        }
    }

    private void refill() {
        long now = System.nanoTime();
        long nanosSinceLastRefill = now - this.lastRefillNanotime;
        if (nanosSinceLastRefill <= nanosToGenerateToken) {
            return;
        }
        long tokensSinceLastRefill = nanosSinceLastRefill / nanosToGenerateToken;
        availableTokens = Math.min(capacity, availableTokens + tokensSinceLastRefill);
        lastRefillNanotime += tokensSinceLastRefill * nanosToGenerateToken;
    }

}
