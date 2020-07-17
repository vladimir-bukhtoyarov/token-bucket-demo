package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._1_ignite_token_bucket;

import com.github.vladimir_bukhtoyarov.token_bucket_demo.RateLimiter;
import org.apache.ignite.IgniteCache;

import java.time.Duration;

public class IgniteTokenBucket implements RateLimiter {

    private final BucketParams bucketParams;
    private final IgniteCache<String, BucketState> cache;
    private final String key;

    public IgniteTokenBucket(long permits, Duration period,
             String key, IgniteCache<String, BucketState> cache) {
        this.bucketParams = new BucketParams(permits, period);
        this.key = key;
        this.cache = cache;
    }

    @Override
    public boolean tryAcquire(int permits) {
        AcquireTokensProcessor processor = new AcquireTokensProcessor();
        return cache.invoke(key, processor, permits, bucketParams);
    }

}
