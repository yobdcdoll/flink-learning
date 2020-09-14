package com.yobdcdoll.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import com.yobdcdoll.source.OrderKafkaDeserializationSchema;
import com.yobdcdoll.model.Order;
import com.yobdcdoll.source.JdbcSourceTest;
import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MetricsTest {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                JdbcSourceTest.class.getResourceAsStream(PropConstant.APPLICATION_PROPERTIES)
        ).mergeWith(ParameterTool.fromArgs(args));

        String bootstrapServers = parameterTool.get("kafka.bootstrap.servers");
        String groupId = parameterTool.get("kafka.group.id");
        String topic = parameterTool.get("kafka.topic.orders");

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
        DataStreamSource<Order> kafkaStream = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> outStream = kafkaStream.map(
                new RichMapFunction<Order, String>() {
                    private transient Counter counter;
                    private transient Long gaugeNum;
                    private transient Histogram histogram;
                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MetricGroup metricGroup = getRuntimeContext().getMetricGroup()
                                .addGroup("yobdcdoll.metricstest");

                        counter = metricGroup.counter("counter01");
                        metricGroup.gauge("gauge01", new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return gaugeNum;
                            }
                        });

                        gaugeNum = 0L;


                        com.codahale.metrics.Histogram dropwizardHistogram =
                                new com.codahale.metrics.Histogram(
                                        new SlidingWindowReservoir(500)
                                );
                        histogram = metricGroup
                                .histogram(
                                        "histogram01"
                                        , new DropwizardHistogramWrapper(dropwizardHistogram)
                                );

                        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
                        meter = metricGroup
                                .meter("meter01", new DropwizardMeterWrapper(dropwizardMeter));
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                    @Override
                    public String map(Order value) throws Exception {
                        counter.inc();
                        gaugeNum++;
                        histogram.update(System.currentTimeMillis() - value.getCreateTime());
                        meter.markEvent();

                        return value.toString();
                    }
                });

        outStream.print();

        env.execute("MetricsTest");
    }
}
