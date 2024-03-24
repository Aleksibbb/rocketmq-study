package com.aleksib.demo;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

import static com.aleksib.constant.MqConstant.NAME_SRV_ADDR;

public class SyncTest {

    /**
     * 生产者发送消息
     */
    @Test
    public void syncProducer() throws Exception {
        // 1. 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("test-producer-group");
        // 2. 设置 NameServer地址
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        // 3. 启动 producer
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 4. 创建一个消息
            Message message = new Message("testTopic", "发送一条测试消息".getBytes());
            // 5. 发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult.getSendStatus());
        }
        // 6. 关闭生产者
        producer.shutdown();
    }

    /**
     * 消费者消费
     */
    @Test
    public void syncConsumer() throws Exception {
        // 1. 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        // 2. 链接 NameServer
        consumer.setNamesrvAddr(NAME_SRV_ADDR);
        // 3. 订阅一个主题 （* 表示订阅这个主题中的所有消息，后面会讲消息过滤）
        consumer.subscribe("testTopic", "*");
        // 4. 设置一个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 实际业务逻辑
                System.out.println("我是消费者");
                msgs.forEach(message -> System.out.println("消息内容：" + new String(message.getBody())));
                System.out.println("消费上下文：" + context);
                // 返回值：CONSUME_SUCCESS 成功，消息会出队
                // RECONSUME_LATER 失败，消息会重新回到队列，稍后重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5. 启动 consumer
        consumer.start();
        // 6. 挂起当前 Jvm
        System.in.read();
    }
}
