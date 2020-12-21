package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._2_ignite_async_token_bucket.remote;

import java.io.Serializable;

public final class BucketState implements Serializable {

    long availableTokens;
    long lastRefillNanotime;

    public BucketState(long availableTokens, long lastRefillNanotime) {
        this.availableTokens = availableTokens;
        this.lastRefillNanotime = lastRefillNanotime;
    }

    public BucketState copy() {
        return new BucketState(availableTokens, lastRefillNanotime);
    }

    public void refill(BucketParams params, long now) {
        long nanosSinceLastRefill = now - lastRefillNanotime;
        if (nanosSinceLastRefill <= params.nanosToGenerateToken) {
            return;
        }
        long tokensSinceLastRefill = nanosSinceLastRefill / params.nanosToGenerateToken;
        availableTokens = Math.min(params.capacity, availableTokens + tokensSinceLastRefill);
        lastRefillNanotime += tokensSinceLastRefill * params.nanosToGenerateToken;
    }

}
