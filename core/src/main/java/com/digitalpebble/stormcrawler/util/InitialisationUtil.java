package com.digitalpebble.stormcrawler.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class InitialisationUtil {

    private static String buildNotAssignableClassExceptionText(
            String qualifiedClassName, Class<?> assignable) {
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

    /**
     * Initializes a class from
     *
     * @param qualifiedClassName
     * @param superclass
     * @param assignables
     * @param <T>
     * @return
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromQualifiedName(
            String qualifiedClassName, Class<? extends T> superclass, Class<?>... assignables) {
        try {
            Class<?> clazz = Class.forName(qualifiedClassName);
            if (clazz.isInterface()) {
                throw new NotAssignableClassException(qualifiedClassName + " is an interface!");
            } else if (Modifier.isAbstract(clazz.getModifiers())) {
                throw new NotAssignableClassException(
                        qualifiedClassName + " is an abstract class!");
            } else if (clazz.isPrimitive()) {
                throw new NotAssignableClassException(qualifiedClassName + " is an primitive!");
            }

            if (!superclass.isAssignableFrom(clazz)) {
                throw new NotAssignableClassException(
                        buildNotAssignableClassExceptionText(qualifiedClassName, superclass));
            }

            for (Class<?> assignable : assignables) {
                if (!assignable.isAssignableFrom(clazz)) {
                    throw new NotAssignableClassException(
                            buildNotAssignableClassExceptionText(qualifiedClassName, assignable));
                }
            }

            final Constructor<?> declaredConstructor = clazz.getDeclaredConstructor();
            final Object o = declaredConstructor.newInstance();
            return superclass.cast(o);
        } catch (ClassNotFoundException e) {
            throw new InitialisationException(
                    "The class " + qualifiedClassName + " was not found!", e);
        } catch (InvocationTargetException e) {
            throw new InitialisationException("The underlying constructor threw an exception.", e);
        } catch (InstantiationException e) {
            throw new NoAssignableConstructorFoundException("The underlying class is abstract.", e);
        } catch (IllegalAccessException e) {
            throw new NoAssignableConstructorFoundException(
                    "The underlying object enforces java language access control and underlying constructor in inaccessible.",
                    e);
        } catch (NoSuchMethodException e) {
            throw new NoAssignableConstructorFoundException(
                    "There was no empty constructor found for " + qualifiedClassName + ".", e);
        }
    }

    static class InitialisationException extends RuntimeException {
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

    static class NotAssignableClassException extends InitialisationException {
        public NotAssignableClassException(String message) {
            super(message);
        }
    }

    static class NoAssignableConstructorFoundException extends InitialisationException {
        public NoAssignableConstructorFoundException(String message) {
            super(message);
        }

        public NoAssignableConstructorFoundException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
