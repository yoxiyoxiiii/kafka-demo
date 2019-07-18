package com.example.kafkademo.hello;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 自定义分区器
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取kafka 集群所有的分区信息
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        Random random = new Random();
        //随机策略
        int partition = random.nextInt(partitionInfos.size());
        return partition;//返回分区index
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
