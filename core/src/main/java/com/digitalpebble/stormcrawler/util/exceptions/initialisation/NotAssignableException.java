package com.digitalpebble.stormcrawler.util.exceptions.initialisation;

import java.lang.reflect.Modifier;
import org.jetbrains.annotations.NotNull;

public class NotAssignableException extends InitialisationException {
    public NotAssignableException(String message) {
        super(message);
    }

    public NotAssignableException(
            @NotNull String qualifiedClassName, @NotNull Class<?> assignable) {
        super(buildNotAssignableExceptionText(qualifiedClassName, assignable));
    }

    public NotAssignableException(String message, Throwable cause) {
        super(message, cause);
    }

    private static String buildNotAssignableExceptionText(
            @NotNull String qualifiedClassName, @NotNull Class<?> assignable) {
        StringBuilder sb = new StringBuilder();
        sb.append("Class ").append(qualifiedClassName).append(" must ");
        String classOrInterface;
        String implementOrExtend;
        if (assignable.isInterface()) {
            classOrInterface = "interface";
            implementOrExtend = "implement";
        } else if (Modifier.isAbstract(assignable.getModifiers())) {
            classOrInterface = "abstract class";
            implementOrExtend = "extend";
        } else {
            classOrInterface = "class";
            implementOrExtend = "extend";
        }
        sb.append(implementOrExtend).append(" the ").append(classOrInterface);
        return sb.toString();
    }
}
