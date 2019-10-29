package com.princeli.kafka.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author : princeli
 * @version 1.0
 * @className SpringKafkaConsumerDemo
 * @date 2019/10/28 9:37 下午
 * @description: TODO
 */
@Component
public class SpringKafkaConsumerDemo {

    @KafkaListener(topics = {"test"})
    public void listener(ConsumerRecord record){
        Optional msg =  Optional.ofNullable(record.value());
        if (msg.isPresent()){
            System.out.println(msg.get());
        }
    }
}
