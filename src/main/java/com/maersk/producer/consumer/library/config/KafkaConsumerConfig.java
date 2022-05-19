package com.maersk.producer.consumer.library.config;

//import com.maersk.producer.consumer.library.kafka.CustomKafkaListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
//@Configuration
public class KafkaConsumerConfig <T> {

    @Autowired
    private ApplicationContext context;

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id:}")
    private String consumerGroup;
    @Value("${kafka.notification.topic:}")
    private String consumerTopic;


    @Bean
    public KafkaMessageListenerContainer<String, T> messageListenerContainer() {

        ContainerProperties containerProperties = new ContainerProperties(getTopic());
        //containerProperties.setMessageListener(new CustomKafkaListener<String, T>());

        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties());
        KafkaMessageListenerContainer<String, T> listenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        //listenerContainer.setAutoStartup(flas);
        // bean name is the prefix of kafka consumer thread name
        listenerContainer.setBeanName("kafka-message-listener");
        return listenerContainer;
    }

    @Bean
    //@ConditionalOnProperty(value = "kafka.notification.topic")
    public Consumer<String, T> createConsumer()
    {
        final Consumer<String, T> consumer = new KafkaConsumer<>(consumerProperties());
        consumer.subscribe(Collections.singletonList(getTopic()));
        return consumer;
    }

    @Bean
    //@ConditionalOnProperty(value = "kafka.bootstrap-servers")
    public Map<String, Object> consumerProperties(){
        //Properties props = new Properties();
        Map<String, Object> props = new HashMap<>();
        log.info("bootstrapServers : {}", bootstrapServers);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-kafka");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }


    public String getBootstrapServer()
    {
        var bootstrapServer = context.getEnvironment().resolvePlaceholders("${kafka.bootstrap-servers}");
        log.info("bootstrapServer : {}",bootstrapServer);
        return bootstrapServer;
    }

    @Bean
    public String getTopic()
    {
        var topic = context.getEnvironment().resolvePlaceholders("${kafka.notification.topic}");
        log.info("topic : {}",topic);
        return topic;
    }
}
