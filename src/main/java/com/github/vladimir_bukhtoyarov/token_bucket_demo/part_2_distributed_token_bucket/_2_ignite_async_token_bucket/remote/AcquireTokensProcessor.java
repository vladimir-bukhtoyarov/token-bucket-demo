package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._2_ignite_async_token_bucket.remote;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;

public class AcquireTokensProcessor implements Serializable,
        EntryProcessor<String, BucketState, Boolean> {

    @Override
    public Boolean process(MutableEntry<String, BucketState> entry,
           Object... arguments) throws EntryProcessorException {

        final int tokensToConsume = (int) arguments[0];
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
        if (state.availableTokens < tokensToConsume) {
            return false;
        }
        state.availableTokens -= tokensToConsume;
        entry.setValue(state);
        return true;
    }

}
