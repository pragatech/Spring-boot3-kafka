package dev.praga.dispatch.exception;

public class NonRetryableException extends RuntimeException{

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(Exception e){
        super(e);
    }
}
