package com.github.vladimir_bukhtoyarov.token_bucket_basics_demo._2_basic_token_bucket;

import com.github.vladimir_bukhtoyarov.token_bucket_basics_demo.RateLimiter;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StraightforwardTokenBucket implements RateLimiter {

    public StraightforwardTokenBucket(long permits, Duration period,
              ScheduledExecutorService scheduler) {
        this.capacity = permits;
        this.availableTokens = capacity;
        long nanosToGenerateToken = period.toNanos() / permits;
        scheduler.scheduleAtFixedRate(this::addToken,
                nanosToGenerateToken, nanosToGenerateToken, TimeUnit.NANOSECONDS
        );
    }

    private final long capacity;
    private long availableTokens;

    synchronized private void addToken() {
        availableTokens = Math.min(capacity, availableTokens + 1);
    }

    @Override
    synchronized public boolean tryAcquire(int permits) {
        if (availableTokens < permits) {
            return false;
        } else {
            availableTokens -= permits;
            return true;
        }
    }

}
