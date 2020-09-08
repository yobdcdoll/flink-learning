package com.yobdcdoll.async;

import com.yobdcdoll.model.Order;
import com.yobdcdoll.source.JdbcSourceTest;
import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kafka读取order，AsyncIO读取users维度表数据
 */
public class AsyncSourceTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                JdbcSourceTest.class.getResourceAsStream(PropConstant.APPLICATION_PROPERTIES)
        ).mergeWith(ParameterTool.fromArgs(args));

        String bootstrapServers = parameterTool.get("kafka.bootstrap.servers");
        String groupId = parameterTool.get("kafka.group.id");
        String topic = parameterTool.get("kafka.topic");


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<Order> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic
                , new OrderKafkaDeserializationSchema()
                , props
        );
        kafkaConsumer.setStartFromEarliest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStreamSource<Order> kafkaStream = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<Tuple6<Long, Long, String, Integer, String, Long>> outStream =
                AsyncDataStream.unorderedWait(
                        kafkaStream
                        , new OrderUserAsyncFunction("mysql1")
                        , 30
                        , TimeUnit.SECONDS
                        , 10
                );

        outStream.print();

        env.execute("AsyncSourceTest");
    }
}
