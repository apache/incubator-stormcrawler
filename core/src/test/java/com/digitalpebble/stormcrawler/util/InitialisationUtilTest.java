package com.digitalpebble.stormcrawler.util;

import com.digitalpebble.stormcrawler.helper.initialisation.*;
import com.digitalpebble.stormcrawler.helper.initialisation.base.*;
import com.digitalpebble.stormcrawler.util.exceptions.initialisation.*;
import org.junit.Assert;
import org.junit.Test;

public class InitialisationUtilTest {
    @Test
    public void can_initialize_a_simple_class() {
        final SimpleOpenClass simpleOpenClass =
                InitialisationUtil.initializeFromQualifiedName(
                        SimpleOpenClass.class.getName(), SimpleOpenClass.class);
    }

    @Test
    public void can_initialize_an_inherited_class_as_abstract() {
        final AbstractClass abstractClass =
                InitialisationUtil.initializeFromQualifiedName(
                        ClassInheritingFomAbstractAndInterface.class.getName(),
                        AbstractClass.class);
    }

    @Test
    public void can_initialize_an_inherited_class_as_interface() {
        final ITestInterface testInterface =
                InitialisationUtil.initializeFromQualifiedName(
                        ClassInheritingFomAbstractAndInterface.class.getName(),
                        ITestInterface.class);
    }

    @Test
    public void can_initialize_final_class() {
        final FinalClassToInitialize finalClassToInitialize =
                InitialisationUtil.initializeFromQualifiedName(
                        FinalClassToInitialize.class.getName(), FinalClassToInitialize.class);
    }

    @Test
    public void can_initialize_an_inherited_class_as_abstract_and_check_for_interface() {
        final AbstractClass abstractClass =
                InitialisationUtil.initializeFromQualifiedName(
                        ClassInheritingFomAbstractAndInterface.class.getName(),
                        AbstractClass.class,
                        ITestInterface.class);
    }

    @Test
    public void
            can_initialize_an_inherited_class_as_abstract_and_check_for_interface_and_abstract_class() {
        final AbstractClass abstractClass =
                InitialisationUtil.initializeFromQualifiedName(
                        ClassInheritingFromOpenClass.class.getName(),
                        OpenClassWithAbstractClassAndInterface.class,
                        ITestInterface.class,
                        AbstractClass.class);
    }

    @Test
    public void can_initialize_an_class_inheriting_an_open_class() {
        final OpenClassWithAbstractClassAndInterface openClassWithAbstractClassAndInterface =
                InitialisationUtil.initializeFromQualifiedName(
                        ClassInheritingFromOpenClass.class.getName(),
                        OpenClassWithAbstractClassAndInterface.class);
    }

    @Test
    public void can_initialize_an_class_inheriting_an_open_class_as_child_class() {
        final OpenClassWithAbstractClassAndInterface openClassWithAbstractClassAndInterface =
                InitialisationUtil.initializeFromQualifiedName(
                        ClassInheritingFromOpenClass.class.getName(),
                        ClassInheritingFromOpenClass.class);
    }

    @Test
    public void fails_if_class_to_initialize_not_existing() {
        Assert.assertThrows(
                ClassForInitialisationNotFoundException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                "does.nott.exist.MyClass", ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_is_interface() {
        Assert.assertThrows(
                NotInitializeableException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ITestInterface.class.getName(), ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_is_abstract() {
        Assert.assertThrows(
                NotInitializeableException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                AbstractClass.class.getName(), AbstractClass.class));
    }

    @Test
    public void fails_if_superclass_to_initialize_is_primitive() {
        Assert.assertThrows(
                SuperclassIsPrimitiveException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                int.class.getName(), int.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_extending_superclass() {
        Assert.assertThrows(
                SuperclassNotAssignableException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                FinalClassToInitialize.class.getName(), AbstractClass.class));
    }

    @Test
    public void fails_if_qualified_class_name_is_blank() {
        Assert.assertThrows(
                QualifiedClassNameBlankException.class,
                () -> InitialisationUtil.initializeFromQualifiedName("   ", AbstractClass.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_extending_classes_to_test_1() {
        Assert.assertThrows(
                ClassNotAssignableException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ClassInheritingFromAbstractClassOnly.class.getName(),
                                AbstractClass.class,
                                ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_extending_classes_to_test_2() {
        Assert.assertThrows(
                ClassNotAssignableException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ClassInheritingFromAbstractClassOnly.class.getName(),
                                AbstractClass.class,
                                OpenClassWithAbstractClassAndInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_implementing_superclass() {
        Assert.assertThrows(
                SuperclassNotAssignableException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                FinalClassToInitialize.class.getName(), ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_has_no_empty_constructor() {
        Assert.assertThrows(
                NoUsableConstructorFoundException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ClassWithoutValidConstructor.class.getName(),
                                ClassWithoutValidConstructor.class));
    }
}
