package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_1_token_bucket_basics._1_simple_rate_limiter;

import com.github.vladimir_bukhtoyarov.token_bucket_demo.RateLimiter;

import java.time.Duration;
import java.util.LinkedList;

public class SimpleRateLimiter implements RateLimiter {

    public SimpleRateLimiter(long permits, Duration period) {
        this.availablePermits = permits;
        this.periodNanos = period.toNanos();
    }

    private final long periodNanos;
    private long availablePermits;
    private final LinkedList<IssuedPermits> issuedPermits = new LinkedList<>();

    private static final class IssuedPermits {
        private final long permits;
        private final long expirationNanotime;

        private IssuedPermits(long permits, long expirationNanotime) {
            this.permits = permits;
            this.expirationNanotime = expirationNanotime;
        }
    }

    @Override
    synchronized public boolean tryAcquire(int permits) {
        long nanoTime = System.nanoTime();
        clearPreviouslyIssuedPermits(nanoTime);

        if (availablePermits < permits) {
            // has no requested permits
            return false;
        } else {
            long expirationNanoTime = nanoTime + periodNanos;
            issuedPermits.addLast(new IssuedPermits(permits, expirationNanoTime));
            availablePermits -= permits;
            return true;
        }
    }

    private void clearPreviouslyIssuedPermits(long currentNanotime) {
        while (!issuedPermits.isEmpty()) {
            IssuedPermits issue = issuedPermits.getFirst();
            if (currentNanotime > issue.expirationNanotime) {
                availablePermits += issue.permits;
                issuedPermits.removeFirst();
            } else {
                return;
            }
        }
    }

}