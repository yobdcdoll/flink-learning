package com.yobdcdoll.join;

import com.yobdcdoll.model.Order;
import com.yobdcdoll.model.Shipment;
import com.yobdcdoll.source.JdbcSourceTest;
import com.yobdcdoll.source.OrderKafkaDeserializationSchema;
import com.yobdcdoll.source.ShipmentKafkaDeserializationSchema;
import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

/**
 * 订单order和发货shipment做interval join
 */
public class IntervalJoinTest {
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
        orderConsumer.setStartFromEarliest();
        FlinkKafkaConsumer<Shipment> shipmentConsumer = new FlinkKafkaConsumer<>(
                shipmentTopic
                , new ShipmentKafkaDeserializationSchema()
                , props
        );
        shipmentConsumer.setStartFromEarliest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

//        DataStreamSource<Order> orderStream = env.addSource(orderConsumer);
//        DataStreamSource<Shipment> shipmentStream = env.addSource(shipmentConsumer);

        SingleOutputStreamOperator<Order> orderStream = env.addSource(orderConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
                );
        SingleOutputStreamOperator<Shipment> shipmentStream = env.addSource(shipmentConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
                );

        DataStream<String> joinedStream = orderStream
                .keyBy(new KeySelector<Order, Long>() {
                    @Override
                    public Long getKey(Order value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .intervalJoin(shipmentStream.keyBy(new KeySelector<Shipment, Long>() {
                    @Override
                    public Long getKey(Shipment value) throws Exception {
                        return value.getOrderId();
                    }
                }))
                .between(Time.seconds(-1), Time.seconds(30))
                .process(new ProcessJoinFunction<Order, Shipment, String>() {
                    @Override
                    public void processElement(Order order, Shipment shipment, Context ctx, Collector<String> out) throws Exception {
                        SimpleDateFormat fmt = new SimpleDateFormat("HH:MM:ss");
                        Date shipTime = new Date(shipment.getShipmentId());
                        Date orderTime = new Date(order.getOrderId());
                        String result = "orderId="+order.getOrderId()
                                +",orderIdWithShip=" + shipment.getOrderId()
                                +",orderTime=" + fmt.format(orderTime)
                                +",shipmentId=" + shipment.getShipmentId()
                                + ",shipmentTime=" + fmt.format(shipTime);
                        out.collect(result);
                    }
                });
        joinedStream.print();

        env.execute("IntervalJoinTest");

    }
}
