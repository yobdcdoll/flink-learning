package com.yobdcdoll.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Order {
    Long orderId;
    Long userId;
    Long itemId;
    Long createTime;
}
