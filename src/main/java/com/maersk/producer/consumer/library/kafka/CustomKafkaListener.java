/*
package com.maersk.producer.consumer.library.kafka;

import com.maersk.producer.consumer.library.config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;

@Slf4j
@Component
public class CustomKafkaListener<K, T> implements MessageListener<K, T> {

   */
/* public void runConsumer()
    {
        final Consumer<String, T> consumer = new KafkaConsumerConfig<T>().createConsumer();
        final ConsumerRecords<String, T> consumerRecords = consumer.poll(Duration.ofMillis(100));
        consumerRecords.forEach(eachRecord -> log.info("Consumer Record:({}, {}, {}, {})\n",
                eachRecord.key(), eachRecord.value(), eachRecord.partition(), eachRecord.offset()));
        consumer.commitAsync();
        consumer.close();
    }*//*


    @Override
    public void onMessage(ConsumerRecord<K, T> ktConsumerRecord) {
        //
    }

    @Override
    public void onMessage(ConsumerRecord<K, T> consumerRecord, Acknowledgment acknowledgment) {
        //MessageListener.super.onMessage(data, acknowledgment);
        log.info("On receiving message");
        try {
            if (Objects.nonNull(consumerRecord.value()))
            {
                log.info("Received message: {}", consumerRecord.value());
                var clazz = consumerRecord.value().getClass();
                log.info("Message is of type: {}", clazz);
            }
            acknowledgment.acknowledge();
        }
        catch (Exception ex)
        {
            log.error("Exception while reading message");
            acknowledgment.acknowledge();
        }
    }

    @Override
    public void onMessage(ConsumerRecord<K, T> data, Consumer<?, ?> consumer) {
        MessageListener.super.onMessage(data, consumer);
    }

    @Override
    public void onMessage(ConsumerRecord<K, T> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        MessageListener.super.onMessage(data, acknowledgment, consumer);
    }
}
*/
