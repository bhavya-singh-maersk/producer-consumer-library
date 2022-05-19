package com.maersk.producer.consumer.library.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CustomKafkaConsumer <T> {

   // @KafkaListener(topics = "#{'${kafka.listener.topics}'.split(',')}", groupId = "test")
    public void listenMessage(ConsumerRecord<String,T> consumerRecord)
    {
      log.info("Consumed message: {}", consumerRecord.value());
    }
}
