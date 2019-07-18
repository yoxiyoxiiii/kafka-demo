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
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");//默认值
        //enable.auto.commit配置项指定了提交offset的方式为自动提交
        properties.put("enable.auto.commit", "true");//false 手动提交 offset
        //设置自动提交 offset 的时间间隔，默认每隔5s，在调用close() 方法时也会自动提交 offset
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5s");
        //反序列化策略
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //订阅topic
        kafkaConsumer.subscribe(Collections.singletonList("helloTopic"));

        while (true) {
            //pull 消息,等待100ms broker 返回数据，不管broker 是否有数据返回，该方法都会返回
            // ，设置为0 时 poll 方法会立即返回。也可以理解为 定时轮询broker
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

                //fetch.min.bytes 消费者从broker 中获取的记录的最小字节数，
                // broker 在收到消费者的数据请求时，如果可用的数量小于 该指定的值，那么broker 会等到有足够可用的数据时才返回给消费者
                String fetchMinBytes = ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
            }
            kafkaConsumer.commitSync();//手动提交offset，同步提交。在broker 响应之前会阻塞
            kafkaConsumer.commitAsync();//异步提交
        }
    }

    public static void main(String[] args) {
        rev();
    }
}
