package com.yobdcdoll.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Shipment {
    Long shipmentId;
    Long orderId;
    Long createTime;
}
