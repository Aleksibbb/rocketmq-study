package com.aleksib.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订单实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MsgModel {

    /**
     * 订单号
     */
    private String orderNum;

    /**
     * 用户 ID
     */
    private Integer userId;

    /**
     * 描述 (下单、短信、物流）
     */
    private String desc;
}
