package com.example.kafkademo.hello;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * 消费者
 */
public class CustomerHelloMsg {

    public static void rev() {
        Properties properties = new Properties();
        //配置kafka 服务器地址
        properties.put("bootstrap.servers","127.0.0.1:9092");
        //设置消费者的 groupId，同一个分组的消费者只有其中一个消费者能消费到消息
        properties.put("group.id","hello");
        //设置消息 拉取策略
        //earliest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        // latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        // none ：topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //enable.auto.commit配置项指定了提交offset的方式为自动提交
        properties.put("enable.auto.commit", "true");
        //反序列化策略
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //订阅topic
        kafkaConsumer.subscribe(Collections.singletonList("helloTopic"));

        while (true) {
            //pull 消息
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                long offset = record.offset();
                int partition = record.partition();
                String topic = record.topic();
                String key = record.key();
                String value = record.value();
                System.out.println("offset:" + offset);
                System.out.println("partition:" + partition);
                System.out.println("topic:" + topic);
                System.out.println("key:" + key);
                System.out.println("value:" + value);
            }
        }

    }

    public static void main(String[] args) {
        rev();
    }
}
