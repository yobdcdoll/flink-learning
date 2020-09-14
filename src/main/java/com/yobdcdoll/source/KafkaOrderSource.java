package com.yobdcdoll.source;

import com.yobdcdoll.util.PropConstant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders
 * kafka-topics.sh --bootstrap-server localhost:9092 --create --topic shipments
 * 生成订单和物流信息
 */
public class KafkaOrderSource {
    public static void main(String[] args) throws IOException {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                JdbcSourceTest.class.getResourceAsStream(PropConstant.APPLICATION_PROPERTIES)
        ).mergeWith(ParameterTool.fromArgs(args));

        String bootstrapServers = parameterTool.get("kafka.bootstrap.servers");
        String orderTopic = parameterTool.get("kafka.topic.orders");
        String shipmentTopic = parameterTool.get("kafka.topic.shipments");
        Long producerInterval = parameterTool.getLong("kafka.interval");
        Long maxCount = parameterTool.getLong("kafka.maxCount");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer producer = new KafkaProducer(props);

        int count = 0;
        while (count < maxCount) {
            Date now = new Date();
            String order = buildOrder(now);
            producer.send(new ProducerRecord<String, String>(orderTopic, order)
                    , new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println("Send order: " + order);
                        }
                    }
            );

            String shipment = buildShipment(now);
            producer.send(new ProducerRecord<String, String>(shipmentTopic, shipment)
                    , new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println("Send shipment: " + order);
                        }
                    }
            );
            try {
                Thread.sleep(producerInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static String buildOrder(Date currentTime) {
        String splitter = ",";
        Random rand = new Random();
        Date now = currentTime;
        Long orderId = now.getTime();
        int userId = rand.nextInt(100) + 1;
        int itemId = rand.nextInt(1000) + 1;
        Long createTime = now.getTime();

        StringBuilder str = new StringBuilder();
        str.append(orderId);
        str.append(splitter);
        str.append(userId);
        str.append(splitter);
        str.append(itemId);
        str.append(splitter);
        str.append(createTime);

        return str.toString();
    }


    private static String buildShipment(Date currentTime) {
        String splitter = ",";
        Random rand = new Random();
        Date now = currentTime;
        Long orderId = now.getTime();
        Long shipmentId = now.getTime() + rand.nextInt(5000);
        Long createTime = shipmentId;

        StringBuilder str = new StringBuilder();
        str.append(shipmentId);
        str.append(splitter);
        str.append(orderId);
        str.append(splitter);
        str.append(createTime);

        return str.toString();
    }
}
