package com.digitalpebble.stormcrawler.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.TestOnly;

public final class GuardedArithmeticsUtil {
    /** Throws an {@link ArithmeticException} when the {@code index} is below 0. */
    public static int checkIndexOverflow(int index) {
        if (index < 0) {
            throw new ArithmeticException("Index overflow has happened!");
        }
        return index;
    }

    /** Throws an {@link ArithmeticException} when (-) + (-) = (+) or (+) + (+) = (-) */
    @Contract(pure = true)
    private static void checkIfOverflowHappenedBySignumPlus(
            int signumA, int signumB, int signumResult) {
        if (signumA == -1 && signumB == -1 && signumResult == 1) {
            throw new ArithmeticException(
                    "Numeric overflow has happened! (signum changed: (-) + (-) = (+))");
        }
        if (signumA == 1 && signumB == 1 && signumResult == -1) {
            throw new ArithmeticException(
                    "Numeric overflow has happened! (signum changed: (+) + (+) = (-))");
        }
    }

    /** Throws an {@link ArithmeticException} when (-) - (+) = (+) or (+) - (-) = (-) */
    @Contract(pure = true)
    private static void checkIfOverflowHappenedBySignumMinus(
            int signumA, int signumB, int signumResult) {
        if (signumA == -1 && signumB == 1 && signumResult == 1) {
            throw new ArithmeticException(
                    "Numeric overflow has happened! (signum changed: (-) - (+) = (+))");
        }
        if (signumA == 1 && signumB == -1 && signumResult == -1) {
            throw new ArithmeticException(
                    "Numeric overflow has happened! (signum changed: (+) - (-) = (-))");
        }
    }

    /** Throws an {@link ArithmeticException} when (-) + (-) = (+) or (+) + (+) = (-) */
    @Contract(pure = true)
    public static int plus(int a, int b) {
        int result = a + b;
        checkIfOverflowHappenedBySignumPlus(
                Integer.signum(a), Integer.signum(b), Integer.signum(result));
        return result;
    }

    /** Throws an {@link ArithmeticException} when (-) - (+) = (+) or (+) - (-) = (-) */
    @Contract(pure = true)
    public static int minus(int a, int b) {
        int result = a - b;
        checkIfOverflowHappenedBySignumMinus(
                Integer.signum(a), Integer.signum(b), Integer.signum(result));
        return result;
    }

    /** Throws an {@link ArithmeticException} when (-) + (-) = (+) or (+) + (+) = (-) */
    @Contract(pure = true)
    public static long plus(long a, long b) {
        long result = a + b;
        checkIfOverflowHappenedBySignumPlus(Long.signum(a), Long.signum(b), Long.signum(result));
        return result;
    }

    /** Throws an {@link ArithmeticException} when (-) - (+) = (+) or (+) - (-) = (-) */
    @Contract(pure = true)
    public static long minus(long a, long b) {
        long result = a - b;
        checkIfOverflowHappenedBySignumMinus(Long.signum(a), Long.signum(b), Long.signum(result));
        return result;
    }

    /** Throws an {@link ArithmeticException} when (-) + (-) = (+) or (+) + (+) = (-) */
    @Contract(pure = true)
    public static long plus(long a, int b) {
        return plus(a, (long) b);
    }

    /** Throws an {@link ArithmeticException} when (-) + (-) = (+) or (+) + (+) = (-) */
    @Contract(pure = true)
    public static long plus(int a, long b) {
        return plus((long) a, b);
    }

    /** Throws an {@link ArithmeticException} when (-) - (+) = (+) or (+) - (-) = (-) */
    @Contract(pure = true)
    public static long minus(long a, int b) {
        return minus(a, (long) b);
    }

    /** Throws an {@link ArithmeticException} when (-) - (+) = (+) or (+) - (-) = (-) */
    @Contract(pure = true)
    public static long minus(int a, long b) {
        return minus((long) a, b);
    }

    /** Throws an {@link ArithmeticException} when (-)-- = (+) */
    @Contract(pure = true)
    public static int dec(int value) {
        if (value == Integer.MIN_VALUE) {
            throw new ArithmeticException("Numeric overflow has happened! (-)-- = (+)");
        }
        return value - 1;
    }

    /** Throws an {@link ArithmeticException} when (+)++ = (-) */
    @Contract(pure = true)
    public static int inc(int value) {
        if (value == Integer.MAX_VALUE) {
            throw new ArithmeticException("Numeric overflow has happened! (+)++ = (-)");
        }
        return value + 1;
    }

    /** Throws an {@link ArithmeticException} when (-)-- = (+) */
    @Contract(pure = true)
    public static long dec(long value) {
        if (value == Long.MIN_VALUE) {
            throw new ArithmeticException("Numeric overflow has happened! (-)-- = (+)");
        }
        return value - 1L;
    }

    /** Throws an {@link ArithmeticException} when (+)++ = (-) */
    @Contract(pure = true)
    public static long inc(long value) {
        if (value == Long.MAX_VALUE) {
            throw new ArithmeticException("Numeric overflow has happened! (+)++ = (-)");
        }
        return value + 1L;
    }

    @TestOnly
    @Contract(pure = true)
    static void test_checkIfOverflowHappenedBySignumPlus(
            int signumA, int signumB, int signumResult) {
        checkIfOverflowHappenedBySignumPlus(signumA, signumB, signumResult);
    }

    @TestOnly
    @Contract(pure = true)
    static void test_checkIfOverflowHappenedBySignumMinus(
            int signumA, int signumB, int signumResult) {
        checkIfOverflowHappenedBySignumMinus(signumA, signumB, signumResult);
    }

    /*
     * Fast Tests for impl
     */
    public static void main(String[] args) {
        System.out.println("Guarded inc");
        System.out.println("inc(min) = " + GuardedArithmeticsUtil.inc(Integer.MIN_VALUE));
        System.out.println("inc(-1) = " + GuardedArithmeticsUtil.inc(-1));
        System.out.println("inc(0) = " + GuardedArithmeticsUtil.inc(0));
        System.out.println("inc(1) = " + GuardedArithmeticsUtil.inc(1));
        try {
            System.out.println("[err] inc(max) = " + GuardedArithmeticsUtil.inc(Integer.MAX_VALUE));
        } catch (ArithmeticException e) {
            System.out.print("inc(min) = ");
            System.out.println(e.getMessage());
        }

        System.out.println("Guarded dec");
        try {
            System.out.println("[err] dec(min) = " + GuardedArithmeticsUtil.dec(Integer.MIN_VALUE));
        } catch (ArithmeticException e) {
            System.out.print("dec(min) = ");
            System.out.println(e.getMessage());
        }
        System.out.println("dec(-1) = " + GuardedArithmeticsUtil.dec(-1));
        System.out.println("dec(0) = " + GuardedArithmeticsUtil.dec(0));
        System.out.println("dec(1) = " + GuardedArithmeticsUtil.dec(1));
        System.out.println("dec(max) = " + GuardedArithmeticsUtil.dec(Integer.MAX_VALUE));

        System.out.println("Guarded plus");
        System.out.println("0 + 0 = " + GuardedArithmeticsUtil.plus(0, 0));
        System.out.println("1 + -1 = " + GuardedArithmeticsUtil.plus(1, -1));
        System.out.println("-1 + 1 = " + GuardedArithmeticsUtil.plus(-1, 1));

        System.out.println("0 + -9 = " + GuardedArithmeticsUtil.plus(0, -9));
        System.out.println("0 + 9 = " + GuardedArithmeticsUtil.plus(0, 9));

        System.out.println("-1 + 9 = " + GuardedArithmeticsUtil.plus(-1, 9));
        System.out.println("1 + -9 = " + GuardedArithmeticsUtil.plus(1, -9));
        System.out.println("1 + 9 = " + GuardedArithmeticsUtil.plus(1, 9));
        System.out.println("-1 + -9 = " + GuardedArithmeticsUtil.plus(-1, -9));

        System.out.println("min + 0 = " + GuardedArithmeticsUtil.plus(Integer.MIN_VALUE, 0));
        System.out.println("min + 9 = " + GuardedArithmeticsUtil.plus(Integer.MIN_VALUE, 9));
        try {
            System.out.println(
                    "[err] min + -9 = " + GuardedArithmeticsUtil.plus(Integer.MIN_VALUE, -9));
        } catch (ArithmeticException e) {
            System.out.print("min + -9 = ");
            System.out.println(e.getMessage());
        }

        System.out.println("max + 0 = " + GuardedArithmeticsUtil.plus(Integer.MAX_VALUE, 0));
        System.out.println("max + -9 = " + GuardedArithmeticsUtil.plus(Integer.MAX_VALUE, -9));
        try {
            System.out.println(
                    "[err] max + 9 = " + GuardedArithmeticsUtil.plus(Integer.MAX_VALUE, 9));
        } catch (ArithmeticException e) {
            System.out.print("max + 9 = ");
            System.out.println(e.getMessage());
        }

        System.out.println("Guarded plus");
        System.out.println("0 - 0 = " + GuardedArithmeticsUtil.minus(0, 0));
        System.out.println("1 - -1 = " + GuardedArithmeticsUtil.minus(1, -1));
        System.out.println("-1 - 1 = " + GuardedArithmeticsUtil.minus(-1, 1));

        System.out.println("0 - -9 = " + GuardedArithmeticsUtil.minus(0, -9));
        System.out.println("0 - 9 = " + GuardedArithmeticsUtil.minus(0, 9));

        System.out.println("-1 - 9 = " + GuardedArithmeticsUtil.minus(-1, 9));
        System.out.println("1 - -9 = " + GuardedArithmeticsUtil.minus(1, -9));
        System.out.println("1 - 9 = " + GuardedArithmeticsUtil.minus(1, 9));
        System.out.println("-1 - -9 = " + GuardedArithmeticsUtil.minus(-1, -9));

        System.out.println("min - 0 = " + GuardedArithmeticsUtil.minus(Integer.MIN_VALUE, 0));
        try {
            System.out.println(
                    "[err] min - 9 = " + GuardedArithmeticsUtil.minus(Integer.MIN_VALUE, 9));
        } catch (ArithmeticException e) {
            System.out.print("min - -9 = ");
            System.out.println(e.getMessage());
        }
        System.out.println("min - -9 = " + GuardedArithmeticsUtil.minus(Integer.MIN_VALUE, -9));

        System.out.println("max - 0 = " + GuardedArithmeticsUtil.minus(Integer.MAX_VALUE, 0));
        try {
            System.out.println(
                    "[err] max - -9 = " + GuardedArithmeticsUtil.minus(Integer.MAX_VALUE, -9));
        } catch (ArithmeticException e) {
            System.out.print("max - -9 = ");
            System.out.println(e.getMessage());
        }
        System.out.println("max - 9 = " + GuardedArithmeticsUtil.minus(Integer.MAX_VALUE, 9));
    }
}
