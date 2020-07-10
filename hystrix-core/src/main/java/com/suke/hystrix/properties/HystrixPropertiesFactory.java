package com.suke.hystrix.properties;

import com.suke.hystrix.HystrixCommandProperties;

public class HystrixPropertiesFactory {
    public static HystrixCommandProperties getCommandProperties(){
        return new HystrixCommandProperties() {
        };
    }
}
