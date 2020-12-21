package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.decorator;

import com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.remote.BucketState;

import javax.cache.processor.MutableEntry;
import java.util.Objects;

public class MutableEntryDecorator<K> implements MutableEntry<K, BucketState> {

    private BucketState state;
    private boolean stateModified;

    public MutableEntryDecorator(BucketState state) {
        this.state = state;
    }

    @Override
    public BucketState getValue() {
        if (state == null) {
            throw new IllegalStateException("'exists' must be called before 'get'");
        }
        return state;
    }

    @Override
    public void setValue(BucketState value) {
        this.state = Objects.requireNonNull(state);
        this.stateModified = true;
    }

    public boolean isStateModified() {
        return stateModified;
    }

    @Override
    public boolean exists() {
        return state != null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public K getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

}
