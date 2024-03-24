package com.aleksib.demo;

import com.aleksib.entity.MsgModel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.aleksib.constant.MqConstant.NAME_SRV_ADDR;

/**
 * 顺序消费
 */
public class OrderlyTest {

    private List<MsgModel> msgList = Arrays.asList(
            new MsgModel("PDD1234", 1,  "下单"),
            new MsgModel("PDD1234", 1,  "短信"),
            new MsgModel("PDD1234", 1,  "物流"),
            new MsgModel("TB1234", 2,  "下单"),
            new MsgModel("TB1234", 2,  "短信"),
            new MsgModel("TB1234", 2,  "物流")
    );

    @Test
    public void orderlyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("orderly-producer-group");
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        producer.start();
        // 发送顺序消息，要确保有序发送，且发送到同一个队列
        msgList.forEach(msgModel -> {
            Message message = new Message("orderlyTopic", msgModel.toString().getBytes());
            try {
                producer.send(message, new MessageQueueSelector() {
                    // 将相同的订单号的消息发送到相同队列
                    // 实现方式：订单号哈希值 对队列数量取余
                    @Override
                    public MessageQueue select(List<MessageQueue> messageQueues, Message message, Object arg) {
                        int hashCode = arg.toString().hashCode() >= 0 ? arg.toString().hashCode() : arg.toString().hashCode() + Integer.MAX_VALUE;
                        int i = hashCode % messageQueues.size();
                        return messageQueues.get(i);
                    }
                }, msgModel.getOrderNum());  // 这里是传给 arg 的值
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        System.out.println("成功");
        producer.shutdown();
    }

    @Test
    public void orderlyConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderly-consumer-group");
        consumer.setNamesrvAddr(NAME_SRV_ADDR);
        consumer.subscribe("orderlyTopic", "*");
        // MessageListenerConcurrently 并发模式，多线程  重试16次
        // MessageListenerOrderly 顺序模式，单线程 （不是全局多线程，对于每一个组是单线程） 无限重试 Integer.MAX_VALUE
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext consumeOrderlyContext) {
                System.out.println("线程Id:" + Thread.currentThread().getId());
                System.out.println(new String(msgs.get(0).getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }
}
