package com.digitalpebble.stormcrawler.util.exceptions.initialisation;

import org.jetbrains.annotations.NotNull;

public final class SuperclassNotAssignableException extends NotAssignableException {
    public SuperclassNotAssignableException(String message) {
        super(message);
    }

    public SuperclassNotAssignableException(
            @NotNull String qualifiedClassName, @NotNull Class<?> assignable) {
        super(qualifiedClassName, assignable);
    }

    public SuperclassNotAssignableException(String message, Throwable cause) {
        super(message, cause);
    }
}
