package com.digitalpebble.stormcrawler.util.exceptions.initialisation;

public class InitialisationException extends RuntimeException {
    public InitialisationException() {
        super();
    }

    public InitialisationException(String message) {
        super(message);
    }

    public InitialisationException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitialisationException(Throwable cause) {
        super(cause);
    }
}
