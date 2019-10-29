package com.princeli.kafka.kafkademo;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author : princeli
 * @version 1.0
 * @className Mypartition
 * @date 2019/10/29 6:39 下午
 * @description: TODO
 */
public class Mypartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("enter");
        List<PartitionInfo> list = cluster.partitionsForTopic(topic);
        int length = list.size();
        if(key == null){
            Random random = new Random();
            return random.nextInt(length);
        }

        return Math.abs(key.hashCode())%length;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
