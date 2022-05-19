package com.maersk.producer.consumer.library.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaBaseConsumerConfig<T> {//implements KafkaListenerConfigurer {

    //private final LocalValidatorFactoryBean validator;

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;
    @Value("${kafka.username:}")
    private String username;
    @Value("${kafka.password:}")
    private String password;
    @Value("${kafka.password-delete-consumer-group:}")
    private String kafkaDeleteConsumerGroupPassword;
    @Value("${kafka.login-module:org.apache.kafka.common.security.plain.PlainLoginModule}")
    private String loginModule;
    @Value("${kafka.sasl-mechanism:PLAIN}")
    private String saslMechanism;
    @Value("${kafka.truststore-location:}")
    private String truststoreLocation;
    @Value("${kafka.clientId:}")
    private String kafkaClientId;
    @Value("${kafka.truststore-password:}")
    private String truststorePassword;
    @Value("${kafka.producer.acks-config:all}")
    private String producerAcksConfig;
    @Value("${kafka.producer.linger:1}")
    private int producerLinger;
    @Value("${kafka.producer.timeout:30000}")
    private int producerRequestTimeout;
    @Value("${kafka.producer.batch-size:16384}")
    private int producerBatchSize;
    @Value("${kafka.producer.send-buffer:131072}")
    private int producerSendBuffer;
    @Value("${kafka.security-protocol:SASL_SSL}")
    private String securityProtocol;
    @Value("${kafka.consumer.group:}")//MSK.deliveryOrder.consumerGroup.v1
    private String consumerGroup;
    @Value("${kafka.consumer.delete-group:}")//MSK.deliveryOrderDelete.consumerGroup.v1
    private String deleteConsumerGroup;
    @Value("${kafka.consumer.offset-auto-reset:latest}")
    private String consumerOffsetAutoReset;
    @Value("${kafka.consumer.max-poll-records:20}")
    private String consumerMaxPollRecords;
    @Value("${kafka.consumer.concurrency:3}")
    private int consumerConcurrency;
    @Value("${kafka.consumer.retry.max-attempts:3}")
    private int maxRetryAttempts;
    @Value("${kafka.consumer.retry.initial-interval-secs:1}")
    private int retryInitialIntervalSeconds;
    @Value("${kafka.consumer.retry.max-interval-secs:10}")
    private int retryMaxIntervalSeconds;
    private static final FixedBackOff ERROR_HANDLER_BACK_OFF = new FixedBackOff(0, 1);

    private static void addSaslProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol, String loginModule,
                                          String username, String password) {
        log.info("Creating SASL Properties, saslMechanism:{}, securityProtocol:{}, loginModule:{}, username:{}, password:{}", saslMechanism,
                securityProtocol, loginModule, username, password != null ? password.length() : null);
        if (!username.isEmpty()) {
            properties.put("security.protocol", securityProtocol);
            properties.put("sasl.mechanism", saslMechanism);
            properties.put("sasl.jaas.config",
                    loginModule + " required username=\"" + username + "\"" + " password=" + "\"" + password + "\" ;");
        }
    }

    private static void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
        if (!location.isEmpty()) {
            properties.put("ssl.truststore.location", location);
            properties.put("ssl.truststore.password", password);
        }
    }

   // @Bean
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
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientId);

        addSaslProperties(properties, saslMechanism, securityProtocol, loginModule, username, password);
        addTruststoreProperties(properties, truststoreLocation, truststorePassword);

        return new DefaultKafkaProducerFactory<>(properties);
    }


    @Bean
    public ConsumerFactory<String, T> consumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        setCommonFactoryProperties(properties, "test-kafka", password);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    private void setCommonFactoryProperties(Map<String, Object> properties, String consumerGroupName, String password) {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerOffsetAutoReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerMaxPollRecords);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaClientId);
        addSaslProperties(properties, saslMechanism, securityProtocol, loginModule, username, password);
        addTruststoreProperties(properties, truststoreLocation, truststorePassword);
    }

    @Bean
    public KafkaTemplate kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public StringJsonMessageConverter stringJsonMessageConverter(ObjectMapper mapper) {
        return new StringJsonMessageConverter(mapper);
    }



    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, T> kafkaListenerContainerFactory(
            StringJsonMessageConverter messageConverter, KafkaTemplate<String, T> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setMessageConverter(messageConverter);
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(consumerConcurrency);
        factory.setRetryTemplate(retryTemplate());
        //factory.setStatefulRetry(true);
        //factory.setErrorHandler(new SeekToCurrentErrorHandler(recoverer(kafkaTemplate()), ERROR_HANDLER_BACK_OFF));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setRecoveryCallback(context -> {
            log.info("Inside recovery callback : {}", context.getLastThrowable().getMessage());
            ConsumerRecord<String, T> consumerRecord = (ConsumerRecord<String, T>) context.getAttribute(RetryingMessageListenerAdapter.CONTEXT_RECORD);
            kafkaTemplate.send("retry", consumerRecord.value());
            log.info("Message published on retry topic : {}", consumerRecord.value());
            return Optional.empty();
        });
        /*factory.setRecoveryCallback(context -> {
            if ((context.getLastThrowable() instanceof TimeoutException)
                    || (context.getLastThrowable() instanceof RuntimeException))
            {
                log.info("Inside recovery method");
                    ConsumerRecord<String, T> record = (ConsumerRecord) context.getAttribute(RetryingMessageListenerAdapter.CONTEXT_RECORD);
                    kafkaTemplate().send("retry", record.value());
                    ((Acknowledgment)context.getAttribute(RetryingMessageListenerAdapter.CONTEXT_ACKNOWLEDGMENT)).acknowledge();
            }
            else
            {
                throw new RuntimeException("Non-recoverable error");
            }
            return null;
        });
*/
        return factory;
    }

    /*@Bean
    public DeadLetterPublishingRecoverer recoverer(KafkaTemplate template) {
        return new DeadLetterPublishingRecoverer(template);
    }*/

    private RetryPolicy retryPolicy() {
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(TimeoutException.class, true);
        exceptionMap.put(RuntimeException.class, true);
        //return new SimpleRetryPolicy(maxRetryAttempts, exceptionMap, true);
        return new SimpleRetryPolicy(maxRetryAttempts);
    }

    private BackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
        policy.setInitialInterval(retryInitialIntervalSeconds * 1000L);
        policy.setMaxInterval(retryMaxIntervalSeconds * 1000L);
        return policy;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(retryPolicy());
        template.setBackOffPolicy(backOffPolicy());
        return template;
    }

    private ErrorHandler errorHandler() {
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(
                (consumerRecord, e) -> log.error("Failed to recover after all retries, consumerRecord:{}", consumerRecord, e),
                ERROR_HANDLER_BACK_OFF);
        seekToCurrentErrorHandler.setCommitRecovered(true);
        return seekToCurrentErrorHandler;
    }

    /*@Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setValidator(this.validator);
    }*/
}
