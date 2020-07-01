package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_1_token_bucket_basics._2_basic_token_bucket;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StraightforwardTokenBucket {

    private final long capacity;

    private long availableTokens;

    public StraightforwardTokenBucket(long capacity, Duration periodToGenerateOneToken, ScheduledExecutorService scheduler) {
        long nanosToGenerateToken = periodToGenerateOneToken.toNanos();
        scheduler.scheduleAtFixedRate(this::addToken, nanosToGenerateToken, nanosToGenerateToken, TimeUnit.NANOSECONDS);

        this.capacity = capacity;
        this.availableTokens = capacity;
    }

    synchronized private void addToken() {
        availableTokens = Math.min(capacity, availableTokens + 1);
    }

    synchronized public boolean tryConsume(int numberTokens) {
        if (availableTokens < numberTokens) {
            return false;
        } else {
            availableTokens -= numberTokens;
            return true;
        }
    }

}
