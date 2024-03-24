package com.aleksib.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import static com.aleksib.constant.MqConstant.NAME_SRV_ADDR;

/**
 * 单向消息
 */
public class OnewayTest  {
    
    @Test
    public void onewayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("oneway-producer-group");
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("onewayTopic", "日志".getBytes());
        producer.sendOneway(message);
        System.out.println("成功");
        producer.shutdown();
    }
}
