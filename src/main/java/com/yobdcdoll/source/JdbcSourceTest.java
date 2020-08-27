package com.yobdcdoll.source;

import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * 读取数据库表增量数据，上次读取的状态可从checkpoint返回
 */
public class JdbcSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                JdbcSourceTest.class.getResourceAsStream(PropConstant.APPLICATION_PROPERTIES)
        ).mergeWith(ParameterTool.fromArgs(args));
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setStateBackend(new FsStateBackend(parameterTool.get(PropConstant.CHECKPOINT_DIR)));


        DataStreamSource<Row> personSource = env
                .addSource(new JdbcSourceFunction("mysql1"));
        personSource.print();

        env.execute();
    }
}

