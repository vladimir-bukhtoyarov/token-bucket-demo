package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._2_ignite_async_token_bucket;

import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._2_ignite_async_token_bucket.remote.AcquireTokensProcessor;
import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._2_ignite_async_token_bucket.remote.BucketParams;
import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._2_ignite_async_token_bucket.remote.BucketState;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class IgniteAsyncTokenBucket {

    private final BucketParams bucketParams;
    private final IgniteCache<String, BucketState> cache;
    private final String key;

    public IgniteAsyncTokenBucket(long permits, Duration period,
                                  String key, IgniteCache<String, BucketState> cache) {
        this.bucketParams = new BucketParams(permits, period);
        this.key = key;
        this.cache = cache;
    }

    public CompletableFuture<Boolean> tryAcquire(int numberTokens) {
        AcquireTokensProcessor entryProcessor = new AcquireTokensProcessor();
        IgniteFuture<Boolean> igniteFuture = cache.invokeAsync(key, entryProcessor, numberTokens, bucketParams);
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
