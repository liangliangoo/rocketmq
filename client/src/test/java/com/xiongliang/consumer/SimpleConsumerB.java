package com.xiongliang.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @Author 六月
 * @Date 2023/1/7 22:37
 * @Version 1.0
 */
public class SimpleConsumerB {

    public static void main(String[] args) {
        System.out.println("rocketMQConsumer  开始------");
        // 消费目标
        // 声明一个消费者consumer，需要传入一个组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-a");
        // 设置集群的NameServer地址，多个地址之间以分号分隔
        consumer.setNamesrvAddr("192.168.124.4:9876");
        // 设置consumer的消费策略
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 集群模式消费，广播消费不会重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 设置最大重试次数，默认是16次
        consumer.setMaxReconsumeTimes(5);
        // 设置consumer所订阅的Topic和Tag，*代表全部的Tag
        try {
            consumer.subscribe("test_topicB", "*");
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        // Listener，主要进行消息的逻辑处理,监听topic，如果有消息就会立即去消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 获取第一条消息，进行处理
                try {
                    if (msgs != null && msgs.size() > 0) {
                        MessageExt messageExt = msgs.get(0);
                        String msgBody = new String(messageExt.getBody(), "utf-8");
                        System.out.println("consumerB 接收消息整体为：" + msgBody);
                    }
                } catch (Exception e) {
                    System.out.println("消息消费失败，请尝试重试！！！");
                    e.printStackTrace();
                    // 尝试重新消费，直接第三次如果还不成功就放弃消费，进行消息消费失败补偿操作
                    if (msgs.get(0).getReconsumeTimes() == 3) {
                        System.out.println("消息记录日志：" + msgs);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } else {
                        // 重试状态码，重试机制可配置
                        // System.out.println("消息消费失败，尝试重试！！！");
                        System.out.println("消息消费失败，请尝试重试！！！");
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                System.out.println("消息消费成功！！！");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 调用start()方法启动consumer
        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        System.out.println("消费者启动成功。。。");
        System.out.println("rocketMQConsumer 结束------");
    }
}

