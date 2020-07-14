package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_1_token_bucket_basics._1_simple_rate_limiter;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleRateLimiterTest {

    private static final int WARMUP_SECONDS = 3;

    public static void main(String[] args) throws InterruptedException {
        // 100 tokens per 1 second
        SimpleRateLimiter limiter = new SimpleRateLimiter(100, Duration.ofSeconds(1));

        AtomicLong consumed = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        initLogging(consumed, rejected);

        while (true) {
            if (limiter.tryAcquire(1)) {
                consumed.addAndGet(1);
            } else {
                rejected.addAndGet(1);
            }
        }
    }

    private static void initLogging(AtomicLong consumed, AtomicLong rejected) {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            System.out.printf("Consumed %d, Rejected %d\n", consumed.getAndSet(0), rejected.getAndSet(0));
        }, WARMUP_SECONDS, 1L, TimeUnit.SECONDS);
    }

}
