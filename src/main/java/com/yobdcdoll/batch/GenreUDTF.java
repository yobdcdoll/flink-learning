package com.yobdcdoll.batch;

import org.apache.flink.table.functions.TableFunction;

public class GenreUDTF extends TableFunction<String> {

    public void eval(String str) {
        if (str == null) {
            return;
        }
        String[] split = str.split("\\|");
        for (int i = 0; split != null && i < split.length; i++) {
            collect(split[i]);
        }
    }
}
