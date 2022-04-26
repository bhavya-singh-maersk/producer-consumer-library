package com.maersk.producer.consumer.library.services;

import com.maersk.producer.consumer.library.exception.MessagePublishException;
import com.maersk.producer.consumer.library.services.KafkaProducerService;
import com.maersk.producer.consumer.library.services.MessagePublisherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class KafkaProducerServiceImpl<T> implements KafkaProducerService<T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private MessagePublisherService <T> messagePublisherService;

    @Autowired
    private Environment env;

    private static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    private static final String RETRY_TOPIC = "${kafka.retry.topic}";
    private static final String KAFKA_HEADERS = "${kafka.headers.keys}";

    @Override
    @Retryable(value = MessagePublishException.class, maxAttemptsExpression = "${spring.retry.maximum.attempts}")
    public void sendMessage(T message, T kafkaHeader) {
        log.info("Publish message: {}", message);
        try {
            int num =5/0;
            var producerTopic = context.getEnvironment().resolvePlaceholders(NOTIFICATION_TOPIC);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            if (Objects.nonNull(kafkaHeader))
                addSingleHeader(producerRecord.headers(), kafkaHeader);
            messagePublisherService.publishOnTopic(producerRecord);
        }
        catch(Exception ex)
        {
            log.error("Exception: ", ex);
            throw new MessagePublishException(ex.getMessage());
        }
    }

    @Override
    public void sendMessage(T message, Map<String, Object> kafkaHeader) {
        try {
            var producerTopic = context.getEnvironment().resolvePlaceholders(NOTIFICATION_TOPIC);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            addHeaders(producerRecord.headers(), kafkaHeader);
            messagePublisherService.publishOnTopic(producerRecord);
        }
        catch(Exception ex)
        {
            log.error("Exception: ", ex);
            throw new MessagePublishException(ex.getMessage());
        }
    }

    @Recover
    public void recover(MessagePublishException exception, T message, T kafkaHeader)
    {
        log.info("Reached recover method");
        var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC);
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
        messagePublisherService.publishOnTopic(producerRecord);
    }

    private void addSingleHeader(Headers headers, Object kafkaHeader)
    {
        var headerFromAppContext = context.getEnvironment().resolvePlaceholders(KAFKA_HEADERS);
        log.info("headersFromAppContext: {}", headerFromAppContext);
        headers.add(headerFromAppContext, kafkaHeader.toString().getBytes(StandardCharsets.UTF_8));
        log.info("headers after loop: {}", headers);
    }

    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader)
    {
        kafkaHeader.forEach((k,v) -> {
            headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8));
        });
    }



}
