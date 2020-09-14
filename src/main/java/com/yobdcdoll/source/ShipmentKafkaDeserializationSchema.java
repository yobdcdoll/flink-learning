package com.yobdcdoll.source;

import com.yobdcdoll.model.Order;
import com.yobdcdoll.model.Shipment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ShipmentKafkaDeserializationSchema implements KafkaDeserializationSchema<Shipment> {
    @Override
    public boolean isEndOfStream(Shipment nextElement) {
        return false;
    }

    @Override
    public Shipment deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        byte[] valBytes = record.value();
        if (valBytes == null) {
            return null;
        }
        String value = new String(valBytes);

        String[] strs = value.split(",");
        if (strs == null || strs.length != 3) {
            return null;
        }
        Long shipmentId = Long.parseLong(strs[0]);
        Long orderId = Long.parseLong(strs[1]);
        Long createTime = Long.parseLong(strs[2]);
        return new Shipment(shipmentId, orderId, createTime);
    }

    @Override
    public TypeInformation<Shipment> getProducedType() {
        return TypeInformation.of(Shipment.class);
    }
}
