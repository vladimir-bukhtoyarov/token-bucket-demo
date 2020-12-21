package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._3_ignite_async_token_batching_decorator.decorator.mock;

import java.util.List;

public class CombinedResult {

    private final List<?> results;

    public CombinedResult(List<?> results) {
        this.results = results;
    }

    public <T> List<T> getResults() {
        return (List<T>) results;
    }

}
