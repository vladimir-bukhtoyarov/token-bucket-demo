package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._1_ignite_token_bucket.remote;

import org.jetbrains.annotations.NotNull;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class AcquireTokensProcessor implements Serializable, EntryProcessor<String, BucketState, Boolean> {
    @Override
    public Boolean process(MutableEntry<String, BucketState> entry, Object... args)
            throws EntryProcessorException {
        final int tokensToConsume = (int) args[0];
        final BucketParams params = (BucketParams) args[1];
        long now = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());

        BucketState state = entry.exists()? new BucketState(entry.getValue()) : new BucketState(params, now);
        state.refill(params, now);

        if (state.availableTokens < tokensToConsume) {
            return false;
        }
        state.availableTokens -= tokensToConsume;
        entry.setValue(state);
        return true;
    }
}















