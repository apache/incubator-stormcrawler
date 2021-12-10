package com.digitalpebble.stormcrawler.util;

import org.junit.Assert;
import org.junit.Test;

public class GuardTest {
    @Test
    public void test_if_plus_guard_for_positive_fails() {
        Assert.assertThrows(
                "A plus action with two positive inputs can never result in something negative!",
                ArithmeticException.class,
                () -> GuardedArithmeticsUtil.test_checkIfOverflowHappenedBySignumPlus(1, 1, -1)
        );
    }

    @Test
    public void test_if_plus_guard_for_negative_fails() {
        Assert.assertThrows(
                "A plus action with two negative inputs can never result in something positive!",
                ArithmeticException.class,
                () -> GuardedArithmeticsUtil.test_checkIfOverflowHappenedBySignumPlus(-1, -1, 1)
        );
    }

    @Test
    public void test_if_minus_guard_for_positive_fails() {
        Assert.assertThrows(
                "A minus action with (-) - (+) inputs can never result in something positive!",
                ArithmeticException.class,
                () -> GuardedArithmeticsUtil.test_checkIfOverflowHappenedBySignumMinus(-1, 1, 1)
        );
    }

    @Test
    public void test_if_minus_guard_for_negative_fails() {
        Assert.assertThrows(
                "A minus action with (+) - (-) inputs can never result in something negative!",
                ArithmeticException.class,
                () -> GuardedArithmeticsUtil.test_checkIfOverflowHappenedBySignumMinus(1, -1, -1)
        );
    }
}
