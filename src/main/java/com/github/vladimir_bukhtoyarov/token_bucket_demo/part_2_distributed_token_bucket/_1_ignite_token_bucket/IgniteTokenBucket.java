package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._1_ignite_token_bucket;

import org.apache.ignite.IgniteCache;

import javax.cache.processor.EntryProcessor;
import java.time.Duration;

public class IgniteTokenBucket {

    private final IgniteCache<String, BucketState> cache;
    private final EntryProcessor<String, BucketState, Boolean> entryProcessor;
    private final String key;

    public IgniteTokenBucket(String key, long capacity, Duration periodToGenerateOneToken, IgniteCache<String, BucketState> cache) {
        this.key = key;
        this.entryProcessor = new TokenBucketProcessor(capacity, periodToGenerateOneToken.toNanos());
        this.cache = cache;
    }

    public boolean tryConsume(int numberTokens) {
        return cache.invoke(key, entryProcessor, numberTokens);
    }

}
