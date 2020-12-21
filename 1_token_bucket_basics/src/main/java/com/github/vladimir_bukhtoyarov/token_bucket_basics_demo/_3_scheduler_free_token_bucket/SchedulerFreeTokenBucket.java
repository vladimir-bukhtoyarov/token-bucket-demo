package com.github.vladimir_bukhtoyarov.token_bucket_basics_demo._3_scheduler_free_token_bucket;

import com.github.vladimir_bukhtoyarov.token_bucket_basics_demo.RateLimiter;

import java.time.Duration;

public class SchedulerFreeTokenBucket implements RateLimiter {

    private final long capacity;
    private long availableTokens;

    private final long nanosToGenerateToken;
    private long lastRefillNanotime;

    public SchedulerFreeTokenBucket(long permits, Duration period) {
        this.nanosToGenerateToken = period.toNanos() / permits;
        this.lastRefillNanotime = System.nanoTime();

        this.capacity = permits;
        this.availableTokens = permits;
    }

    @Override
    synchronized public boolean tryAcquire(int permits) {
        refill();
        if (availableTokens < permits) {
            return false;
        } else {
            availableTokens -= permits;
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
