package com.aleksib.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import static com.aleksib.constant.MqConstant.NAME_SRV_ADDR;

/**
 * 异步消息
 */
public class ASyncTest {
    /**
     * 生产者发送消息
     */
    @Test
    public void asyncProducer() throws Exception {
        // 1. 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("async-producer-group");
        // 2. 设置 NameServer地址
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        // 3. 启动 producer
        producer.start();
        // 4. 创建一个消息
        Message message = new Message("asyncTopic", "发送一条异步消息".getBytes());
        // 5. 发送消息
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送成功");
            }
            @Override
            public void onException(Throwable e) {
                System.out.println("发送失败：" + e.getMessage());
            }
        });
        System.out.println("已经发送");
        System.in.read();
    }
}
