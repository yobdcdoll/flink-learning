package com.yobdcdoll.async;

import com.yobdcdoll.model.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrderKafkaDeserializationSchema implements KafkaDeserializationSchema<Order> {
    @Override
    public boolean isEndOfStream(Order nextElement) {
        return false;
    }

    @Override
    public Order deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        byte[] valBytes = record.value();
        if(valBytes==null){
            return null;
        }
        String value = new String(valBytes);

        String[] strs = value.split(",");
        if (strs == null || strs.length != 4) {
            return null;
        }
        Long orderId = Long.parseLong(strs[0]);
        Long userId = Long.parseLong(strs[1]);
        Long itemId = Long.parseLong(strs[2]);
        Long createTime = Long.parseLong(strs[3]);
        return new Order(orderId, userId, itemId, createTime);
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}
