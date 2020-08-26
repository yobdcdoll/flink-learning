package com.yobdcdoll.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class JdbcSourceFunction<T> extends RichSourceFunction<T> implements CheckpointedFunction {
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        while (isRunning) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (sourceContext.getCheckpointLock()) {
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
