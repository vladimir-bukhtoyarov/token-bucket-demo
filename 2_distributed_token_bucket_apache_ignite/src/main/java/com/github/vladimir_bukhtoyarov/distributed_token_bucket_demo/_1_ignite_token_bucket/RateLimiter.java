package com.github.vladimir_bukhtoyarov.distributed_token_bucket_demo._1_ignite_token_bucket;

public interface RateLimiter {

    boolean tryAcquire(int permits);

}









