package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator;

import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.decorator.BatchHelper;
import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.remote.BatchAcquireProcessor;
import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.remote.BucketParams;
import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.remote.BucketState;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class IgniteAsyncBatchingTokenBucket {

    private final BucketParams bucketParams;
    private final IgniteCache<String, BucketState> cache;
    private final String key;

    private final BatchHelper<Long, Boolean, List<Long>, List<Boolean>> batchHelper =
            BatchHelper.async(this::invokeBatch);

    public IgniteAsyncBatchingTokenBucket(long permits, Duration period, String key,
                                          IgniteCache<String, BucketState> cache) {
        this.bucketParams = new BucketParams(permits, period);
        this.key = key;
        this.cache = cache;
    }

    public CompletableFuture<Boolean> tryAcquire(long numberTokens) {
        return batchHelper.executeAsync(numberTokens);
    }

    private CompletableFuture<List<Boolean>> invokeBatch(List<Long> commands) {
        IgniteFuture<List<Boolean>> future = cache.invokeAsync(key, new BatchAcquireProcessor(), commands, bucketParams);
        return convertFuture(future);
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
