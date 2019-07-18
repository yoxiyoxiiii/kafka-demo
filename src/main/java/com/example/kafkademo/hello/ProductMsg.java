package com.example.kafkademo.hello;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 消息生产者
 * 消息发送策略：
 * 1.fire-and-forget ：发送并忘记，只将消息投递出去 ，并不关心是否能到达broker
 * 2.同步发送：send() 方法发送消息，返回Future 对象，get返回会阻塞，就可以知道消息是否投递成功
 * 3.异步发送：send（）方法设置 异步回调，服务器在
 *
 */
public class ProductMsg  {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        sendMsg();
    }

    public static void sendMsg() throws ExecutionException, InterruptedException {
         Properties kafkaProperties = new Properties();

         //kafka broker 地址
         kafkaProperties.put("bootstrap.servers","127.0.0.1:9092");
         //key 序列化策略
         kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
         //value 序列化策略
         kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //配置分区器，不配置的话采用默认的分区器 DefaultPartitioner
         kafkaProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.example.kafkademo.hello.MyPartitioner");
         // 消息投递者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);
        //ProducerRecord 消息对象包装者 设置topic , partition ,key, value(消息体)
        // key 计算hash 值，得到 该消息该被投递到的分区，key 相同的消息被投递到同一个分区
        // 当key 为空时，使用默认的分区器，采用轮询算发依次将消息投递到各个分区
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("helloTopic",0,"helloKey","helloValue");
        //消息成功投递到broker 返回 RecordMetadata 对象。
        Future<RecordMetadata> metadataFuture = producer.send(producerRecord);//消息同步发送
        RecordMetadata recordMetadata = metadataFuture.get();
        //消息异步发送
        Future<RecordMetadata> metadataFuture2 = producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("消息异步发送，broker 回调");
                System.out.println(metadata.offset());
            }
        });//消息同步发送

        producer.close();
    }
}
