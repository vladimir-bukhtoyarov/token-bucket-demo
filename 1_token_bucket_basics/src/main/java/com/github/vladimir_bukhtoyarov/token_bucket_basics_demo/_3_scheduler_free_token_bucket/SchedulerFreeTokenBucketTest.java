package com.github.vladimir_bukhtoyarov.token_bucket_basics_demo._3_scheduler_free_token_bucket;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SchedulerFreeTokenBucketTest {

    private static final int WARMUP_SECONDS = 3;

    public static void main(String[] args) throws InterruptedException {
        // 100 tokens per 1 second
        SchedulerFreeTokenBucket limiter = new SchedulerFreeTokenBucket(100L, Duration.ofSeconds(1));

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
