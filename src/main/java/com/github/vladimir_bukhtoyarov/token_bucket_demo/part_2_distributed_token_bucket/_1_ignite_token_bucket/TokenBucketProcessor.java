package com.github.vladimir_bukhtoyarov.token_bucket_demo.part_2_distributed_token_bucket._1_ignite_token_bucket;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

public class TokenBucketProcessor implements EntryProcessor<String, BucketState, Boolean> {

    private final long capacity;
    private final long nanosToGenerateToken;

    public TokenBucketProcessor(long capacity, long nanosToGenerateToken) {
        this.capacity = capacity;
        this.nanosToGenerateToken = nanosToGenerateToken;
    }

    @Override
    public Boolean process(MutableEntry<String, BucketState> entry, Object... arguments) throws EntryProcessorException {
        int tokensToConsume = (Integer) arguments[0];

        BucketState state;
        if (!entry.exists()) {
            state = new BucketState(capacity, System.currentTimeMillis() * 1_000_000L);
        } else {
            BucketState persistedState = entry.getValue();
            state = persistedState.copy();
            refill(state);
        }

        if (state.availableTokens < tokensToConsume) {
            return false;
        }

        state.availableTokens -= tokensToConsume;
        entry.setValue(state);
        return true;
    }

    private void refill(BucketState state) {
        long now = System.currentTimeMillis() * 1_000_000L;
        long nanosSinceLastRefill = now - state.lastRefillNanotime;
        if (nanosSinceLastRefill <= nanosToGenerateToken) {
            return;
        }
        long tokensSinceLastRefill = nanosSinceLastRefill / nanosToGenerateToken;
        state.availableTokens = Math.min(capacity, state.availableTokens + tokensSinceLastRefill);
        state.lastRefillNanotime += tokensSinceLastRefill * nanosToGenerateToken;
    }

}
