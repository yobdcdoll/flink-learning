package com.yobdcdoll.batch;

import org.apache.flink.table.functions.ScalarFunction;

public class UpperCaseUDF extends ScalarFunction {
    public String eval(String s) {
        if (s != null) {
            return s.toUpperCase();
        }
        return null;
    }
}
