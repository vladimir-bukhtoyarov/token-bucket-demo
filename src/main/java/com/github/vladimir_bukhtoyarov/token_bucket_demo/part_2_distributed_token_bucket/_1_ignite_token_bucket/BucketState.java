package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._1_ignite_token_bucket;

public final class BucketState {

    long availableTokens;
    long lastRefillNanotime;

    public BucketState(long availableTokens, long lastRefillNanotime) {
        this.availableTokens = availableTokens;
        this.lastRefillNanotime = lastRefillNanotime;
    }

    public BucketState copy() {
        return new BucketState(availableTokens, lastRefillNanotime);
    }

}
