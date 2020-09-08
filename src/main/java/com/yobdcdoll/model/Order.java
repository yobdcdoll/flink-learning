package com.yobdcdoll.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Order {
    Long orderId;
    Long userId;
    Long itemId;
    Long createTime;
}
