package com.maersk.producer.consumer.library.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maersk.producer.consumer.library.exception.MessagePublishException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

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

    @Autowired
    private StorageService storageService;

    @Autowired
    private StorageConnectionFactory storageAccountConnector;

    private static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    private static final String RETRY_TOPIC = "${kafka.retry.topic}";
    private static final String KAFKA_HEADERS = "${kafka.headers.keys}";
    private static final String PAYLOAD_STORAGE_TYPE = "${events-payload.storage-type}";
    private static final String MAX_ALLOWED_PAYLOAD_SIZE = "${events-payload.max-bytes}";

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
        ProducerRecord<String, T> producerRecord = null;
        try {
            var producerTopic = context.getEnvironment().resolvePlaceholders(NOTIFICATION_TOPIC);
            if (isLargePayload(message))
            {
                var storageType = context.getEnvironment().resolvePlaceholders(PAYLOAD_STORAGE_TYPE);
                log.info("storageType : {}" ,storageType);
                var payloadReference = storageAccountConnector.getStorageService(storageType)
                        .storePayloadToCloud(message);
                log.info("payloadReference : {}" ,payloadReference);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("reference", payloadReference);
                ObjectMapper mapper = new ObjectMapper();
                log.info("payload: {}", jsonObject.toString());
                producerRecord = new ProducerRecord<>(producerTopic, (T) jsonObject.toString());
                producerRecord.headers().add("isLargePayload", "YES".getBytes(StandardCharsets.UTF_8));
            }
            else
            {
                producerRecord = new ProducerRecord<>(producerTopic, message);
            }
            addHeaders(producerRecord.headers(), kafkaHeader);
            messagePublisherService.publishOnTopic(producerRecord);
        }
        catch(Exception ex)
        {
            log.error("Exception: ", ex);
            throw new MessagePublishException(ex.getMessage());
        }
    }

    @Override
    public void sendMessage(String eventName, T message, Map<String, Object> kafkaHeader) {

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
        kafkaHeader.forEach((k,v) -> headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private boolean isLargePayload(T message)
    {
        var payloadSize = message.toString().getBytes(StandardCharsets.UTF_8).length;
        log.info("payloadSize: {}", payloadSize);
        var maxAllowedSize = context.getEnvironment().resolvePlaceholders(MAX_ALLOWED_PAYLOAD_SIZE);
        return payloadSize > Integer.parseInt(maxAllowedSize);
    }

}
