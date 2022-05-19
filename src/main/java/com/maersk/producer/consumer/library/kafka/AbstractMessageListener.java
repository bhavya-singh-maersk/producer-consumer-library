package com.maersk.producer.consumer.library.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Slf4j
public class AbstractMessageListener<T> {//implements GenericMessageListener<T>{

   // private String kafkaTopic;

   /* protected AbstractMessageListener(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }*/

    //@Override
    public void readMessage(ConsumerRecord<String, T> consumerRecord) {
      log.info("Consumed record : {}", consumerRecord);
    }

   // protected abstract void specificLogic(ConsumerRecord<String, String> consumerRecord);

    /*public String getKafkaTopic() {
        return kafkaTopic;
    }*/
}
