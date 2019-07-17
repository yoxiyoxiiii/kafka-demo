package com.example.kafkademo.hello;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 消息生产者
 */
public class ProductMsg  {

    public static void main(String[] args) {
        sendMsg();
    }

    public static void sendMsg() {
         Properties kafkaProperties = new Properties();
         //kafka broker 地址
         kafkaProperties.put("bootstrap.servers","127.0.0.1:9092");
         //key 序列化策略
         kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
         //value 序列化策略
         kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
         // 消息投递者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);
        //ProducerRecord 消息对象包装者 设置topic , partition ,key, value(消息体)
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("helloTopic",0,"helloKey","helloValue");
        producer.send(producerRecord);
        producer.close();
    }
}
