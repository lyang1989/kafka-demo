package com.princeli.kafka.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author : princeli
 * @version 1.0
 * @className KafkaProducer
 * @date 2019/10/28 7:57 下午
 * @description: TODO
 */
public class KafkaProducerDemo extends Thread{

    KafkaProducer<Integer,String> producer;
    String topic;

    public KafkaProducerDemo(String topic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"prince-producer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.princeli.kafka.kafkademo.Mypartition");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,10);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,3000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        producer = new KafkaProducer<Integer, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        int num = 0;
        String message = "prince kafka demo message";

        while (num <20){
            try {

                //同步阻塞
                //RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, message)).get();
                //System.out.println(recordMetadata.offset() + "->" + recordMetadata.partition() + "->" + recordMetadata.topic());

                //回调通知-异步
                producer.send(new ProducerRecord<>(topic, message),(metadata,exception)->{
                    System.out.println(metadata.offset() + "->" + metadata.partition() + "->" + metadata.topic());
                });



                TimeUnit.SECONDS.sleep(2);
                ++num;
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        new KafkaProducerDemo("test_partition").start();
    }
}
