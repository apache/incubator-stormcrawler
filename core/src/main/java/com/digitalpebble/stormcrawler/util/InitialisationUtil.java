package com.digitalpebble.stormcrawler.util;

import com.digitalpebble.stormcrawler.util.exceptions.initialisation.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class InitialisationUtil {

    /**
     * Initializes a class from {@code qualifiedClassName} as type {@code superClass}. Further
     * constrains for implemented classes and interfaces are possible via {@code
     * furtherSuperClasses}. Requirements: {@code qualifiedClassName}: 1. Has an accessible empty
     * constructor. 2. Points to a class extending/implementing {@code superClass}. 3. Points to a
     * class extending/implementing all {@code furtherSuperClasses}. 4. Points to a class that is
     * not primitive, abstract or an interface. 5. Points to an existing class. 6. Is not blank.
     *
     * <p>{@code superClass}: 1. Does not point to a primitive 2. Points to a class
     * extending/implemented by the class of {@code qualifiedClassName}.
     *
     * <p>{@code furtherSuperClasses} 1. Points to classes extending/implemented by the class of
     * {@code qualifiedClassName}.
     *
     * @param qualifiedClassName the qualified name for the class to be initialized.
     * @param superClass defines the type to be instantiated
     * @param furtherSuperClasses further checks for specific interfaces etc.
     * @param <T> the type of the returned class.
     * @return an instance of {@code qualifiedClassName} of type {@code superClass}
     *     extending/implementing all {@code furtherSuperClasses}.
     * @throws SuperclassNotAssignableException if the {@code superClass} is not assignable to the
     *     class of {@code clazz}
     * @throws ClassNotAssignableException if any of the {@code furtherSuperClasses} is not
     *     assignable to the class of {@code clazz}
     * @throws SuperclassIsPrimitiveException if the {@code superClass} is a primitive
     * @throws ClassForInitialisationNotFoundException if the {@code clazz} is not pointing to a
     *     class.
     * @throws QualifiedClassNameBlankException if the {@code clazz} is blank.
     * @throws NoUsableConstructorFoundException if either the underlying constructor is
     *     inaccessbible, no empty constructor found or the underlying class is somehow abstract.
     * @throws InitialisationException if the underlying constructor threw an exception.
     * @throws NotInitializeableException if the {@code clazz} points to a interface, abstract class
     *     or primitive.
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromQualifiedName(
            @NotNull String qualifiedClassName,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        return initializeFromClassUnchecked(
                getClassFor(qualifiedClassName, superClass, furtherSuperClasses));
    }

    /**
     * Retrieves a class-instance for {@code qualifiedClassName} extending {@code T}. {@code T} is
     * supplied by contraining the type of {@code qualifiedClassName} to {@code superClass}. Further
     * constrains for implemented classes and interfaces are possible via {@code
     * furtherSuperClasses}. Requirements: {@code clazz}: 1. Points to a class
     * extending/implementing {@code superClass}. 2. Points to a class extending/implementing all
     * {@code furtherSuperClasses}. 3. Points to a class that is not primitive, abstract or an
     * interface. 4. Points to an existing class. 5. Is not blank.
     *
     * <p>{@code superClass}: 1. Does not point to a primitive 2. Points to a class
     * extending/implemented by the class of {@code clazz}.
     *
     * <p>{@code furtherSuperClasses} 1. Points to classes extending/implemented by the class of
     * {@code clazz}.
     *
     * @param qualifiedClassName the qualified name for the class to be retrieved.
     * @param superClass defines the type to be instantiated
     * @param furtherSuperClasses further checks for specific interfaces etc.
     * @param <T> the type extended by the returned class.
     * @return an instance of {@code clazz} of type {@code superClass} extending/implementing all
     *     {@code furtherSuperClasses}.
     * @throws SuperclassNotAssignableException if the {@code superClass} is not assignable to the
     *     class of {@code clazz}
     * @throws ClassNotAssignableException if any of the {@code furtherSuperClasses} is not
     *     assignable to the class of {@code clazz}
     * @throws SuperclassIsPrimitiveException if the {@code superClass} is a primitive
     * @throws ClassForInitialisationNotFoundException if the {@code clazz} is not pointing to a
     *     class.
     * @throws QualifiedClassNameBlankException if the {@code clazz} is blank.
     */
    @NotNull
    @Contract(pure = true)
    public static <T> Class<? extends T> getClassFor(
            @NotNull String qualifiedClassName,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        if (StringUtils.isBlank(qualifiedClassName)) {
            throw new QualifiedClassNameBlankException("The qualified class name is empty!");
        }

        checkSuperClass(superClass);

        try {
            Class<?> clazz = Class.forName(qualifiedClassName);
            return requireSuperClass(clazz, superClass, furtherSuperClasses);
        } catch (ClassNotFoundException e) {
            throw new ClassForInitialisationNotFoundException(
                    "The class " + qualifiedClassName + " was not found!", e);
        }
    }

    /**
     * Initializes a class from {@code clazz} as type {@code superClass}. Further constrains for
     * implemented classes and interfaces are possible via {@code furtherSuperClasses}.
     * Requirements: {@code clazz}: 1. Has an accessible empty constructor. 2. Points to a class
     * extending/implementing {@code superClass}. 3. Points to a class extending/implementing all
     * {@code furtherSuperClasses}. 4. Points to a class that is not primitive, abstract or an
     * interface. 5. Points to an existing class. 6. Is not blank.
     *
     * <p>{@code superClass}: 1. Does not point to a primitive 2. Points to a class
     * extending/implemented by the class of {@code clazz}.
     *
     * <p>{@code furtherSuperClasses} 1. Points to classes extending/implemented by the class of
     * {@code clazz}.
     *
     * @param clazz the class to be initialized.
     * @param superClass defines the type to be instantiated
     * @param furtherSuperClasses further checks for specific interfaces etc.
     * @param <T> the type of the returned class.
     * @return an instance of {@code clazz} of type {@code superClass} extending/implementing all
     *     {@code furtherSuperClasses}.
     * @throws NoUsableConstructorFoundException if either the underlying constructor is
     *     inaccessbible, no empty constructor found or the underlying class is somehow abstract.
     * @throws InitialisationException if the underlying constructor threw an exception.
     * @throws NotInitializeableException if the {@code clazz} points to a interface, abstract class
     *     or primitive.
     * @throws SuperclassNotAssignableException if the {@code superClass} is not assignable to the
     *     class of {@code clazz}
     * @throws ClassNotAssignableException if any of the {@code furtherSuperClasses} is not
     *     assignable to the class of {@code clazz}
     * @throws SuperclassIsPrimitiveException if the {@code superClass} is a primitive
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromClass(
            @NotNull Class<?> clazz,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        return initializeFromClassUnchecked(
                requireSuperClass(clazz, superClass, furtherSuperClasses));
    }

    /**
     * Initializes a class from {@code clazz} of type {@code T}.
     *
     * @param clazz the class to be initialized.
     * @param <T> the type of the returned class.
     * @return an instance of {@code clazz}.
     * @throws NoUsableConstructorFoundException if either the underlying constructor is
     *     inaccessbible, no empty constructor found or the underlying class is somehow abstract.
     * @throws InitialisationException if the underlying constructor threw an exception.
     * @throws NotInitializeableException if the {@code clazz} points to a interface, abstract class
     *     or primitive.
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromClass(@NotNull Class<? extends T> clazz) {
        checkClazzSimple(clazz);
        return initializeFromClassUnchecked(clazz);
    }

    /*
     * The unchecked method to initialize
     */
    @NotNull
    @Contract(pure = true)
    private static <T> T initializeFromClassUnchecked(@NotNull Class<? extends T> clazz) {
        try {
            final Constructor<? extends T> declaredConstructor = clazz.getDeclaredConstructor();
            return declaredConstructor.newInstance();
        } catch (InvocationTargetException e) {
            throw new InitialisationException("The underlying constructor threw an exception.", e);
        } catch (InstantiationException e) {
            throw new NoUsableConstructorFoundException("The underlying class is abstract.", e);
        } catch (IllegalAccessException e) {
            throw new NoUsableConstructorFoundException(
                    "The underlying object enforces java language access control and underlying constructor in inaccessible.",
                    e);
        } catch (NoSuchMethodException e) {
            throw new NoUsableConstructorFoundException(
                    "There was no empty constructor found for " + clazz.getName() + ".", e);
        }
    }

    /**
     * Asserts the following: {@code clazz}: 1. Points to a class extending/implementing {@code
     * superClass}. 2. Points to a class extending/implementing all {@code furtherSuperClasses}. 3.
     * Points to a class that is not primitive, abstract or an interface.
     *
     * <p>{@code superClass}: 1. Does not point to a primitive 2. Points to a class
     * extending/implemented by the class of {@code clazz}.
     *
     * <p>{@code furtherSuperClasses} 1. Points to classes extending/implemented by the class of
     * {@code clazz}.
     *
     * @return Either fails or returns {@code clazz} cast to Class&lt;? extends T&gt;
     * @throws NotInitializeableException if the {@code clazz} points to a interface, abstract class
     *     or primitive.
     * @throws SuperclassNotAssignableException if the {@code superClass} is not assignable to the
     *     class of {@code clazz}
     * @throws ClassNotAssignableException if any of the {@code furtherSuperClasses} is not
     *     assignable to the class of {@code clazz}
     * @throws SuperclassIsPrimitiveException if the {@code superClass} is a primitive
     */
    @SuppressWarnings("unchecked")
    @Contract(pure = true)
    public static <T> Class<? extends T> requireSuperClass(
            @NotNull Class<?> clazz,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        checkSuperClass(superClass);

        checkClazzSimple(clazz);

        if (!superClass.isAssignableFrom(clazz)) {
            throw new SuperclassNotAssignableException(clazz.getName(), superClass);
        }

        for (Class<?> assignable : furtherSuperClasses) {
            if (!assignable.isAssignableFrom(clazz)) {
                throw new ClassNotAssignableException(clazz.getName(), assignable);
            }
        }

        return (Class<? extends T>) clazz;
    }

    /**
     * Asserts the following: {@code superClass}: 1. Does not point to a primitive
     *
     * @throws SuperclassIsPrimitiveException if the {@code superClass} is a primitive
     */
    private static void checkSuperClass(@NotNull Class<?> superClass) {
        if (superClass.isPrimitive()) {
            throw new SuperclassIsPrimitiveException(
                    "The superClass" + superClass.getName() + " is an primitive!");
        }
    }

    /**
     * Asserts the following: {@code clazz}: 1. Points to a class that is not primitive, abstract or
     * an interface.
     *
     * @throws NotInitializeableException if the {@code clazz} points to a interface, abstract class
     *     or primitive.
     */
    @Contract(pure = true)
    private static void checkClazzSimple(@NotNull Class<?> clazz) {
        if (clazz.isInterface()) {
            throw new NotInitializeableException(clazz.getName() + " is an interface!");
        } else if (Modifier.isAbstract(clazz.getModifiers())) {
            throw new NotInitializeableException(clazz.getName() + " is an abstract class!");
        } else if (clazz.isPrimitive()) {
            throw new NotInitializeableException(clazz.getName() + " is an primitive!");
        }
    }

    private InitialisationUtil() {}
}
