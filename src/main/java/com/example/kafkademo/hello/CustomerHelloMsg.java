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
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id","hello");
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
