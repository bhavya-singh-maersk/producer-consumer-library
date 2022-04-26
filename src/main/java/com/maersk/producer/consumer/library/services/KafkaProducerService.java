package com.maersk.producer.consumer.library.services;

import java.util.Map;

public interface KafkaProducerService <T> {

    void sendMessage(T message, T kafkaHeader);

    void sendMessage(T message, Map<String,Object> kafkaHeader);
}
