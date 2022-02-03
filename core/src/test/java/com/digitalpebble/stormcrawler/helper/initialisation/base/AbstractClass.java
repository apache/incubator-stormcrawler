package com.digitalpebble.stormcrawler.helper.initialisation.base;

public abstract class AbstractClass {
    private int variable;

    public AbstractClass() {
        variable = 0;
    }

    public AbstractClass(int variable) {
        this.variable = variable;
    }

    public int getVariable() {
        return variable;
    }

    public void setVariable(int variable) {
        this.variable = variable;
    }
}
