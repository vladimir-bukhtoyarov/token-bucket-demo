package com.github.vladimir_bukhtoyarov.token_bucket_basics_demo._4_lock_free_token_bucket;

import java.time.Duration;
















public class BucketParams {

    final long capacity;
    final long nanosToGenerateToken;

    public BucketParams(long permits, Duration period) {
        this.nanosToGenerateToken = period.toNanos() / permits;
        this.capacity = permits;
    }

}


































