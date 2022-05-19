package com.maersk.producer.consumer.library.services;

import com.maersk.producer.consumer.library.exception.MessagePublishException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.net.ConnectException;

@Slf4j
@Component
@NoArgsConstructor
@AllArgsConstructor
public class MessagePublisherService<T>{

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    @Autowired
    private ApplicationContext context;

    private static final String RETRY_TOPIC = "${kafka.retry.topic}";

   @Retryable(value = {MessagePublishException.class, Exception.class}, maxAttemptsExpression = "${spring.retry.maximum.attempts}")
    public void publishMessageToKafka(ProducerRecord<String, T> producerRecord)
    {
        log.info("Inside publishMessageToKafka");
        try {
            ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
            future.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, T> result) {
                   //log.info("Sent message=[{}] with offset=[{}]", producerRecord.value(), result.getRecordMetadata().offset());
                    log.info("Sent message with offset=[{}]", result.getRecordMetadata().offset());
                }

                @Override
                public void onFailure(Throwable ex) {
                 log.error("Unable to send message=[{}] due to : {}", producerRecord.value(), ex);
                }
            });
        } catch (Exception exception)
        {
            log.error("Exception while posting message to topic", exception);
            if (exception instanceof TimeoutException || exception instanceof TransactionTimedOutException)
            {
                throw new MessagePublishException(exception.getMessage());
            }
            throw exception;
        }
    }

    @Recover
    public void recover(MessagePublishException exception, ProducerRecord<String, T> producerRecord)
    {
        log.info("Reached recover method for MessagePublishException");
        var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC);
        var updateProducerRecord = new ProducerRecord<String, T>(retryTopic, producerRecord.value());
       // log.info("updateProducerRecord: {}", updateProducerRecord.value());
        ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(updateProducerRecord);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, T> result) {
                log.info("Sent message to retry topic=[{}] with offset=[{}]", updateProducerRecord.value(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message to retry topic=[{}] due to : {}", updateProducerRecord.value(), ex);
            }
        });
    }

    @Recover
    public void recover(Exception exception, ProducerRecord<String, T> producerRecord)
    {
        log.info("Reached recover method for Arithmetic exception");
       // var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC);
        var updateProducerRecord = new ProducerRecord<String, T>("dlt", producerRecord.value());
        //log.info("updateProducerRecord: {}", updateProducerRecord.value());
        ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(updateProducerRecord);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, T> result) {
                log.info("Sent message to retry topic=[{}] with offset=[{}]", updateProducerRecord.value(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message to retry topic=[{}] due to : {}", updateProducerRecord.value(), ex);
            }
        });
    }



    public void publishOnTopic(ProducerRecord<String, T> producerRecord)
    {
        log.info("Inside publishOnTopic");
        ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, T> result) {
                log.info("Sent message=[{}] with offset=[{}]", producerRecord.value(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message=[{}] due to : {}", producerRecord.value(), ex);
            }
        });
        log.info("Completed publishOnTopic");
    }
}
