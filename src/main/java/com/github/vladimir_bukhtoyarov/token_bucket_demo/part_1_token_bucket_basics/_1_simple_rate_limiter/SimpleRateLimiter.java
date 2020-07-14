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
    private final LinkedList<IssuedPermits> issuedTokens = new LinkedList<>();

    private static final class IssuedPermits {
        private final long permits;
        private final long expirationNanotime;

        private IssuedPermits(long permits, long expirationNanotime) {
            this.permits = permits;
            this.expirationNanotime = expirationNanotime;
        }
    }

    @Override
    synchronized public boolean tryAcquire(int numberTokens) {
        long nanoTime = System.nanoTime();
        clearPreviouslyIssuedPermits(nanoTime);

        if (availablePermits < numberTokens) {
            // has no requested permits
            return false;
        } else {
            long expirationNanoTime = nanoTime + periodNanos;
            issuedTokens.addLast(new IssuedPermits(numberTokens, expirationNanoTime));
            availablePermits -= numberTokens;
            return true;
        }
    }

    private void clearPreviouslyIssuedPermits(long currentNanotime) {
        while (!issuedTokens.isEmpty()) {
            IssuedPermits issue = issuedTokens.getFirst();
            if (currentNanotime > issue.expirationNanotime) {
                availablePermits += issue.permits;
                issuedTokens.removeFirst();
            } else {
                return;
            }
        }
    }

}