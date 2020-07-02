package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._2_ignite_async_token_bucket;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

import javax.cache.processor.EntryProcessor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class IgniteTokenBucket {

    private final IgniteCache<String, BucketState> cache;
    private final EntryProcessor<String, BucketState, Boolean> entryProcessor;
    private final String key;

    public IgniteTokenBucket(String key, long capacity, Duration periodToGenerateOneToken, IgniteCache<String, BucketState> cache) {
        this.key = key;
        this.entryProcessor = new TokenBucketProcessor(capacity, periodToGenerateOneToken.toNanos());
        this.cache = cache;
    }

    public CompletableFuture<Boolean> tryConsume(int numberTokens) {
        IgniteFuture<Boolean> igniteFuture = cache.invokeAsync(key, entryProcessor, numberTokens);
        return convertFuture(igniteFuture);
    }

    private static <T> CompletableFuture<T> convertFuture(IgniteFuture<T> igniteFuture) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        igniteFuture.listen((IgniteInClosure<IgniteFuture<T>>) completedIgniteFuture -> {
            try {
                completableFuture.complete(completedIgniteFuture.get());
            } catch (Throwable t) {
                completableFuture.completeExceptionally(t);
            }
        });
        return completableFuture;
    }
}
