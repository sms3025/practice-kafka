package com.practice.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling   // DeviceLogSimulator 의 @Scheduled 를 위해
public class PracticeKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(PracticeKafkaApplication.class, args);
    }
}
