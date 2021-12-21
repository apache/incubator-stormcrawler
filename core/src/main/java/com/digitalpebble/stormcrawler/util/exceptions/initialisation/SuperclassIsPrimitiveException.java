package com.digitalpebble.stormcrawler.util.exceptions.initialisation;

import org.jetbrains.annotations.NotNull;

public final class SuperclassIsPrimitiveException extends NotAssignableException {
    public SuperclassIsPrimitiveException(String message) {
        super(message);
    }

    public SuperclassIsPrimitiveException(
            @NotNull String qualifiedClassName, @NotNull Class<?> assignable) {
        super(qualifiedClassName, assignable);
    }
}
