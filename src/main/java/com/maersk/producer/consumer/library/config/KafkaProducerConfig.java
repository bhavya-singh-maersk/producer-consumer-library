package com.maersk.producer.consumer.library.config;

import com.maersk.producer.consumer.library.exception.MessagePublishException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.TransactionTimedOutException;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;


public class KafkaProducerConfig <T> {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id:}")
    private String consumerGroup;
    @Value("${kafka.notification.topic:}")
    private String consumerTopic;
    @Value("${kafka.producer.linger:1}")
    private int producerLinger;
    @Value("${kafka.producer.timeout:30000}")
    private int producerRequestTimeout;
    @Value("${kafka.producer.batch-size:16384}")
    private int producerBatchSize;
    @Value("${kafka.producer.send-buffer:131072}")
    private int producerSendBuffer;
    @Value("${kafka.producer.acks-config:all}")
    private String producerAcksConfig;
    @Value("${kafka.consumer.retry.initial-interval-secs:1}")
    private int retryInitialIntervalSeconds;
    @Value("${kafka.consumer.retry.max-interval-secs:10}")
    private int retryMaxIntervalSeconds;

    @Bean
    public ProducerFactory<String, T> producerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeout);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.ACKS_CONFIG, producerAcksConfig);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-kafka");

        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaTemplate<String,T> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public RetryPolicy retryPolicy()
    {
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(MessagePublishException.class, true);
        retryableExceptions.put(ConnectException.class, true);
        retryableExceptions.put(TransactionTimedOutException.class, true);
        retryableExceptions.put(TimeoutException.class, true);
        SimpleRetryPolicy policy = new SimpleRetryPolicy(3, retryableExceptions);
        policy.setMaxAttempts(3);
        return policy;
    }

    @Bean
    public BackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
        policy.setInitialInterval(retryInitialIntervalSeconds * 1000L);
        policy.setMaxInterval(retryMaxIntervalSeconds * 1000L);
        return policy;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();

        template.setRetryPolicy(retryPolicy());
        template.setBackOffPolicy(backOffPolicy());

        return template;

    }
}
