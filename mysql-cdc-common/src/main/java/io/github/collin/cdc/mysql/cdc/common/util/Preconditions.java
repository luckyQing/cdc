package io.github.collin.cdc.mysql.cdc.common.util;

public class Preconditions {

    public static void checkState(boolean condition, Object errorMessage) {
        if (!condition) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

}