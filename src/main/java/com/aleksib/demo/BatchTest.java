package com.aleksib.demo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.aleksib.constant.MqConstant.NAME_SRV_ADDR;

/**
 * 批量消息
 */
public class BatchTest {

    @Test
    public void batchProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("batch-producer-group");
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        producer.start();
        List<Message> msgs = Arrays.asList(
                new Message("batchTopic", "一组消息中的消息A".getBytes()),
                new Message("batchTopic", "一组消息中的消息B".getBytes()),
                new Message("batchTopic", "一组消息中的消息C".getBytes())
        );
        producer.send(msgs);
        producer.shutdown();
    }
}
