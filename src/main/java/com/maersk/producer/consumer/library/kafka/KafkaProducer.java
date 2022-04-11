package com.maersk.producer.consumer.library.kafka;

import com.maersk.producer.consumer.library.exception.MessagePublishException;
import com.maersk.producer.consumer.library.services.MessagePublisherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service
public class KafkaProducer <T> {

    @Autowired
    private ApplicationContext context;
    @Autowired
    private MessagePublisherService <T> messagePublisherService;

    @Autowired
    private Environment env;

    private static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    private static final String RETRY_TOPIC = "${kafka.retry.topic}";
    private static final String KAFKA_HEADERS = "${kafka.headers}";

    //@Retryable(value = MessagePublishException.class, maxAttemptsExpression = "${spring.retry.maximum.attempts}")
    public void sendMessage(T message) {
        log.info("Publish message: {}", message);
        try {
            var producerTopic = context.getEnvironment().resolvePlaceholders(NOTIFICATION_TOPIC);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            producerRecord.headers().add("corId", "corId".getBytes(StandardCharsets.UTF_8));
            addKafkaHeaders(producerRecord.headers());
            //messagePublisherService.publishOnTopic(producerRecord);
        }
        catch(Exception ex)
        {
            log.error("Exception: ", ex);
            throw new MessagePublishException(ex.getMessage());
        }
    }

    @Recover
    public void recover(MessagePublishException exception, T message)
    {
        log.info("Reached recover method");
        var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC);
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
        messagePublisherService.publishOnTopic(producerRecord);
    }

    private void addKafkaHeaders(Headers headers)
    {
        var headersFromAppContext = Arrays.asList(context.getEnvironment().resolvePlaceholders(KAFKA_HEADERS).split(","));
        log.info("headersFromAppContext: {}", headersFromAppContext);
        headersFromAppContext.forEach(header -> headers.add(header, header.getBytes(StandardCharsets.UTF_8)));
    }
}
