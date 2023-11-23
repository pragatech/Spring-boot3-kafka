package dev.praga.dispatch.exception;

public class RetryableException extends RuntimeException{
    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Exception e) {
        super(e);
    }
}
