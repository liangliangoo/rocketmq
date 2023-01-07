package com.xiongliang.producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author xiongliang
 * @version 1.0
 * @description
 * @since 2023/1/7  19:02
 */
public class SimpleProducer {

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("producer-test");
        producer.setNamesrvAddr("121.4.26.248:9876");
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key", System.currentTimeMillis());
        Message test_topic = new Message("test_topic", jsonObject.toJSONString().getBytes());

        try {
            SendResult send = producer.send(test_topic);
            System.out.println(JSONObject.toJSONString(send));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        new Thread(() -> sendMsg("test_topicA", producer)).start();
        new Thread(() -> sendMsg("test_topicB", producer)).start();

    }

    private static void sendMsg(String topic, MQProducer producer) {
        while (true) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("key", topic + System.currentTimeMillis());
            Message test_topic = new Message(topic, jsonObject.toJSONString().getBytes());

            try {
                SendResult send = producer.send(test_topic);
                System.out.println(JSONObject.toJSONString(send));
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
