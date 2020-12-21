package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.decorator.mock;

public interface MockCommand<R> {

    public R apply(MockState state);

}
