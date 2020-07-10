package com.suke.hystrix;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HystrixCommandKeyTest extends HystrixCommand<String> {
    private final String name;

    public HystrixCommandKeyTest(String name) {
        super(HystrixCommandGroupKey.Factory.asKey("ExampleGroup"));
        this.name = name;
    }

    @Override
    protected String run() {
        return "Hello " + name + "!";
    }

    public static class UnitTest {
        @Test
        public void testSynchronous() {
            assertEquals("Hello World!", new HystrixCommandKeyTest("World").execute());
            assertEquals("Hello Bob!", new HystrixCommandKeyTest("Bob").execute());
        }
    }
}
