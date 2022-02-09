/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.util;

import com.digitalpebble.stormcrawler.helper.initialisation.*;
import com.digitalpebble.stormcrawler.helper.initialisation.base.*;
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
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                "does.not.exist.MyClass", ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_is_interface() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ITestInterface.class.getName(), ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_is_abstract() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                AbstractClass.class.getName(), AbstractClass.class));
    }

    @Test
    public void fails_if_superclass_to_initialize_is_primitive() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                int.class.getName(), int.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_extending_superclass() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                FinalClassToInitialize.class.getName(), AbstractClass.class));
    }

    @Test
    public void fails_if_qualified_class_name_is_blank() {
        Assert.assertThrows(
                RuntimeException.class,
                () -> InitialisationUtil.initializeFromQualifiedName("   ", AbstractClass.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_extending_classes_to_test_1() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ClassInheritingFromAbstractClassOnly.class.getName(),
                                AbstractClass.class,
                                ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_extending_classes_to_test_2() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ClassInheritingFromAbstractClassOnly.class.getName(),
                                AbstractClass.class,
                                OpenClassWithAbstractClassAndInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_not_implementing_superclass() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                FinalClassToInitialize.class.getName(), ITestInterface.class));
    }

    @Test
    public void fails_if_class_to_initialize_has_no_empty_constructor() {
        Assert.assertThrows(
                RuntimeException.class,
                () ->
                        InitialisationUtil.initializeFromQualifiedName(
                                ClassWithoutValidConstructor.class.getName(),
                                ClassWithoutValidConstructor.class));
    }
}
