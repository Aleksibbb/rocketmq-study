package com.aleksib.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static com.aleksib.constant.MqConstant.NAME_SRV_ADDR;

/**
 * 延迟消息
 */
public class DelayTest {

    @Test
    public void delayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("delay-producer-group");
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("delayTopic", "延迟消息".getBytes());
        System.out.println("发送时间：" + new Date());
        message.setDelayTimeLevel(3);
        producer.send(message);
        producer.shutdown();
    }

    @Test
    public void delayConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delay-consumer-group");
        consumer.setNamesrvAddr(NAME_SRV_ADDR);
        consumer.subscribe("delayTopic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("收到消息：" + new Date());
                System.out.println("消费内容：" + new String(msgs.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5. 启动 consumer
        consumer.start();
        // 6. 挂起当前 Jvm
        System.in.read();
    }

}
