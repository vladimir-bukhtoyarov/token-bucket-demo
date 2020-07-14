package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._2_ignite_async_token_bucket.remote;

import java.io.Serializable;
import java.time.Duration;

public class BucketParams implements Serializable {

    final long capacity;
    final long nanosToGenerateToken;

    public BucketParams(long capacity, Duration period) {
        this.capacity = capacity;
        this.nanosToGenerateToken = period.toNanos() / capacity;
    }

}