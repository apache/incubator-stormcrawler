package com.digitalpebble.stormcrawler.util.exceptions.initialisation;

import org.jetbrains.annotations.NotNull;

public final class ClassNotAssignableException extends NotAssignableException {

    public ClassNotAssignableException(String message) {
        super(message);
    }

    public ClassNotAssignableException(
            @NotNull String qualifiedClassName, @NotNull Class<?> assignable) {
        super(qualifiedClassName, assignable);
    }
}
