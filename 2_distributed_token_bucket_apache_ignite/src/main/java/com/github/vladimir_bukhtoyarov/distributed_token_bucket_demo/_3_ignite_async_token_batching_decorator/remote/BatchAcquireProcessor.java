package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.remote;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;










public class BatchAcquireProcessor implements Serializable, EntryProcessor<String, BucketState, List<Boolean>> {
    @Override
    public List<Boolean> process(MutableEntry<String, BucketState> entry, Object... arguments)
            throws EntryProcessorException {
        final List<Long> tryConsumeCommands = (List<Long>) arguments[0];
        final BucketParams params = (BucketParams) arguments[1];
        long nanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        BucketState state = entry.exists() ? new BucketState(entry.getValue()) : new BucketState(params, nanoTime);
        state.refill(params, nanoTime);
        List<Boolean> results = new ArrayList<>(tryConsumeCommands.size());
        long consumedTokens = 0;
        for (Long tokensToConsume : tryConsumeCommands) {
            if (state.availableTokens < tokensToConsume) {
                results.add(false);
            } else {
                state.availableTokens -= tokensToConsume;
                results.add(true);
                consumedTokens += tokensToConsume;
            }
        }
        if (consumedTokens > 0) {
            entry.setValue(state);
        }
        return results;
    }
}







