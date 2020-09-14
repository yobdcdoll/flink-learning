package com.yobdcdoll.join;

import com.yobdcdoll.model.Shipment;
import com.yobdcdoll.source.OrderKafkaDeserializationSchema;
import com.yobdcdoll.model.Order;
import com.yobdcdoll.source.JdbcSourceTest;
import com.yobdcdoll.source.ShipmentKafkaDeserializationSchema;
import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;

public class StreamJoinTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                JdbcSourceTest.class.getResourceAsStream(PropConstant.APPLICATION_PROPERTIES)
        ).mergeWith(ParameterTool.fromArgs(args));

        String bootstrapServers = parameterTool.get("kafka.bootstrap.servers");
        String groupId = parameterTool.get("kafka.group.id");
        String orderTopic = parameterTool.get("kafka.topic.orders");
        String shipmentTopic = parameterTool.get("kafka.topic.shipments");

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        FlinkKafkaConsumer<Order> orderConsumer = new FlinkKafkaConsumer<>(
                orderTopic
                , new OrderKafkaDeserializationSchema()
                , props
        );
        FlinkKafkaConsumer<Shipment> shipmentConsumer = new FlinkKafkaConsumer<>(
                shipmentTopic
                , new ShipmentKafkaDeserializationSchema()
                , props
        );
        shipmentConsumer.setStartFromEarliest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Order> orderStream = env.addSource(orderConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
                );
        SingleOutputStreamOperator<Shipment> shipmentStream = env.addSource(shipmentConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
                );

        DataStream<String> joinedStream = orderStream
                .join(shipmentStream)
                .where(new KeySelector<Order, Long>() {
                    @Override
                    public Long getKey(Order value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .equalTo(new KeySelector<Shipment, Long>() {
                    @Override
                    public Long getKey(Shipment value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Order, Shipment, String>() {
                    @Override
                    public String join(Order order, Shipment shipment) throws Exception {
                        return "shipmentId:" + shipment.getShipmentId()
                                + ", orderId:" + order.getOrderId();
                    }
                });
        joinedStream.print();

        env.execute("StreamJoinTest");

    }
}
