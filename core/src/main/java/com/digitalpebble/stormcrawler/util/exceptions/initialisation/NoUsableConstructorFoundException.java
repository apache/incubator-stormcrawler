package com.digitalpebble.stormcrawler.util.exceptions.initialisation;

public class NoUsableConstructorFoundException extends InitialisationException {
    public NoUsableConstructorFoundException(String message) {
        super(message);
    }

    public NoUsableConstructorFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
