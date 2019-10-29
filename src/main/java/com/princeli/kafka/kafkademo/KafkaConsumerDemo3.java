package com.princeli.kafka.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author : princeli
 * @version 1.0
 * @className KafkaConsumer
 * @date 2019/10/28 7:57 下午
 * @description: TODO
 */
public class KafkaConsumerDemo3 extends Thread{

    KafkaConsumer<Integer,String> consumer;
    String topic;

    public KafkaConsumerDemo3(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"prince-producer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"prince-gid");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");//自动提交（批量确认）
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        consumer = new KafkaConsumer<Integer, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singleton(this.topic));
        while (true){
            ConsumerRecords<Integer,String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            consumerRecords.forEach(record ->{
                System.out.println(record.key()+"->"+record.value()+"->"+record.offset());
            });

        }
    }

    public static void main(String[] args) {
        new KafkaConsumerDemo3("test_partition").start();
    }
}
