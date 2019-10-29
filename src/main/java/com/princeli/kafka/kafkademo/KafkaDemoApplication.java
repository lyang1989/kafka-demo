package com.princeli.kafka.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) throws InterruptedException {
       ConfigurableApplicationContext context =  SpringApplication.run(KafkaDemoApplication.class, args);
       SpringKafkaProducerDemo springKafkaProducerDemo = context.getBean(SpringKafkaProducerDemo.class);
        for (int i = 0; i < 10; i++) {
            springKafkaProducerDemo.send();
            TimeUnit.SECONDS.sleep(2);
        }
    }

}
