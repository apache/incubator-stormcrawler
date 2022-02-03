package com.digitalpebble.stormcrawler.helper.initialisation;

import com.digitalpebble.stormcrawler.helper.initialisation.base.AbstractClass;

public class ClassWithoutValidConstructor extends AbstractClass {
    public ClassWithoutValidConstructor(int i) {
        System.out.println(i);
    }
}
