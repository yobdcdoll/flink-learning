package com.yobdcdoll.batch;


import org.apache.flink.table.functions.AggregateFunction;

public class MySumUDAF extends AggregateFunction<Integer, Integer> {
    @Override
    public Integer getValue(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    public void accumulate(Integer accumulator, String input) {
        accumulator++;
    }
}
