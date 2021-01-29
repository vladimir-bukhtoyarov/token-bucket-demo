package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._2_ignite_async_token_bucket.remote;

import java.io.Serializable;

public final class BucketState {

    long availableTokens;
    long lastRefillNanoTime;

    public BucketState(BucketParams params, long nanoTime) {
        this.lastRefillNanoTime = nanoTime;
        this.availableTokens = params.capacity;
    }

    public BucketState(BucketState other) {
        this.lastRefillNanoTime = other.lastRefillNanoTime;
        this.availableTokens = other.availableTokens;
    }

    public void refill(BucketParams params, long nanoTime) {
        long nanosSinceLastRefill = nanoTime - this.lastRefillNanoTime;
        if (nanosSinceLastRefill <= params.nanosToGenerateToken) {
            return;
        }
        long tokensSinceLastRefill = nanosSinceLastRefill / params.nanosToGenerateToken;
        availableTokens = Math.min(params.capacity, availableTokens + tokensSinceLastRefill);
        lastRefillNanoTime += tokensSinceLastRefill * params.nanosToGenerateToken;
    }

}
