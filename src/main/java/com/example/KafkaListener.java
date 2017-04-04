package com.example;


public class KafkaListener {

    @org.springframework.kafka.annotation.KafkaListener(topics = "test")
    public void listen(String message) {
        System.out.println(message);
    }
}
