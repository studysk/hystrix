package com.suke.hystrix;

public interface HystrixExecutable<R> {

    public R execute();
}
