package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.remote;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BatchAcquireProcessor implements Serializable, EntryProcessor<String, BucketState, List<Boolean>> {

    @Override
    public List<Boolean> process(MutableEntry<String, BucketState> entry, Object... arguments) throws EntryProcessorException {
        final List<Long> tryConsumeCommands = (List<Long>) arguments[0];
        final BucketParams params = (BucketParams) arguments[1];
        long now = System.currentTimeMillis() * 1_000_000L;

        BucketState state;
        if (!entry.exists()) {
            state = new BucketState(params.capacity, System.currentTimeMillis() * 1_000_000L);
        } else {
            BucketState persistedState = entry.getValue();
            state = persistedState.copy();
            state.refill(params, now);
        }

        // Execute batch
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

        // save results if something was consumed
        if (consumedTokens > 0) {
            entry.setValue(state);
        }

        return results;
    }

}
