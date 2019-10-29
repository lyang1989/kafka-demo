package com.princeli.kafka.kafkademo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author : princeli
 * @version 1.0
 * @className SpringKafkaProducerDemo
 * @date 2019/10/28 9:38 下午
 * @description: TODO
 */
@Component
public class SpringKafkaProducerDemo {

    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;

    public void send(){
        kafkaTemplate.send("test",1,"msgData");
    }

}
