package com.github.vladimir_bukhtoyarov.token_bucket_demo;

public interface RateLimiter {

    boolean tryAcquire(int permits);

}









