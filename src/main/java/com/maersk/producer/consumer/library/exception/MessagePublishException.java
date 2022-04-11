package com.maersk.producer.consumer.library.exception;

public class MessagePublishException extends RuntimeException {

    private int retryCount;

    public int getRetryCount() {
        return retryCount;
    }

    public MessagePublishException(String message, int retryCount) {
        super(message);
        this.retryCount = retryCount;
    }

    public MessagePublishException(String message) {
        super(message);
    }
}
