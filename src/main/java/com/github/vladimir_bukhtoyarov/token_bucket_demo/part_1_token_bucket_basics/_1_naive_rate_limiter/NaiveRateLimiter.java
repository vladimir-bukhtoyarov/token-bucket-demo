package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_1_token_bucket_basics._1_naive_rate_limiter;

import java.time.Duration;
import java.util.LinkedList;

public class NaiveRateLimiter {

    /**
     * Creates instance of rate limiter which provides guarantee that consumption rate will be >= tokens/periodMillis
     */
    public NaiveRateLimiter(long tokens, Duration period) {
        this.availableTokens = tokens;
        this.periodMillis = period.toMillis();
    }

    private long availableTokens;
    private final long periodMillis;
    private LinkedList<Issue> issuedTokens = new LinkedList<>();

    synchronized public boolean tryConsume(int numberTokens) {
        long nowMillis = System.currentTimeMillis();
        clearObsoleteIssues(nowMillis);

        if (availableTokens < numberTokens) {
            // has no requested tokens in the bucket
            return false;
        } else {
            long expirationTimestamp = nowMillis + periodMillis;
            issuedTokens.addLast(new Issue(numberTokens, expirationTimestamp));
            availableTokens -= numberTokens;
            return true;
        }
    }

    private void clearObsoleteIssues(long nowMillis) {
        while (!issuedTokens.isEmpty()) {
            Issue issue = issuedTokens.getFirst();
            if (nowMillis > issue.expirationTimestamp) {
                availableTokens += issue.tokens;
                issuedTokens.removeFirst();
            } else {
                return;
            }
        }
    }

    private static final class Issue {
        private final long tokens;
        private final long expirationTimestamp;

        private Issue(long tokens, long expirationTimestamp) {
            this.tokens = tokens;
            this.expirationTimestamp = expirationTimestamp;
        }
    }

}