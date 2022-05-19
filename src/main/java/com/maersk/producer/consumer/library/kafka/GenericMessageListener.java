package com.maersk.producer.consumer.library.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface GenericMessageListener <T> {

    String METHOD = "readMessage";

    void readMessage(ConsumerRecord<String, T> consumerRecord);
}
