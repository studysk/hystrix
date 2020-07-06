package com.suke.hystrix;

import com.suke.hystrix.exception.HystrixRuntimeException;

public abstract class HystrixCommand<R> implements HystrixExecutable{
    private  HystrixCircuitBreaker circuitBreaker;


    public R execute() {
        try{
            if(!circuitBreaker.allowRequest()){
                return null;
            }
            R r = executeWithSemaphore();
            return r;
        }catch (Throwable e){
            throw new HystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION, this.getClass(), null, e, null);
        }

    }

    private  R executeWithSemaphore(){
        return null;
    }
}
