package com.maersk.producer.consumer.library.kafka;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;

import java.lang.reflect.Method;
import java.util.*;

@Slf4j
//@Configuration
@AllArgsConstructor
public class KafkaTestConsumerConfig<T> implements KafkaListenerConfigurer {

    @Autowired
    private final List<AbstractMessageListener<T>> listeners;

    @Autowired
    private final BeanFactory beanFactory;

    @Autowired
    private final MessageHandlerMethodFactory messageHandlerMethodFactory;

    @Autowired
    private final KafkaListenerContainerFactory kafkaListenerContainerFactory;

    @Value("${your.kafka.consumer.group-id}")
    private String consumerGroup;

    @Value("${your.application.name}")
    private String service;

    @Override
    public void configureKafkaListeners(
            final KafkaListenerEndpointRegistrar registrar) {

        final Method listenerMethod = lookUpMethod();

        listeners.forEach(listener -> registerListenerEndpoint(listener, listenerMethod, registrar));
    }

    private void registerListenerEndpoint(final AbstractMessageListener<T> listener,
                                          final Method listenerMethod,
                                          final KafkaListenerEndpointRegistrar registrar) {

      //  log.info("Registering {} endpoint on topic {}", listener.getClass(),
       //         listener.getKafkaTopic());

        final MethodKafkaListenerEndpoint<String, String> endpoint =
                createListenerEndpoint(listener, listenerMethod);
        registrar.registerEndpoint(endpoint);
    }

    private MethodKafkaListenerEndpoint<String, String> createListenerEndpoint(
            final AbstractMessageListener<T> listener, final Method listenerMethod) {

        final MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();
        endpoint.setBeanFactory(beanFactory);
        endpoint.setBean(listener);
        endpoint.setMethod(listenerMethod);
        //endpoint.setId(service + "-" + listener.getKafkaTopic());
        endpoint.setGroup(consumerGroup);
        //endpoint.setTopics(listener.getKafkaTopic());
        endpoint.setMessageHandlerMethodFactory(messageHandlerMethodFactory);

        return endpoint;
    }

    private Method lookUpMethod() {
        return Arrays.stream(GenericMessageListener.class.getMethods())
                .filter(m -> m.getName().equals(GenericMessageListener.METHOD))
                .findAny()
                .orElseThrow(() ->
                        new IllegalStateException("Could not find method " + GenericMessageListener.METHOD));
    }

    /*@Bean
    public KafkaMessageListenerContainer<String, T> messageListenerContainer() {

        ContainerProperties containerProperties = new ContainerProperties(getTopic());
        containerProperties.setMessageListener(new AbstractMessageListener<>());
        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties());
        KafkaMessageListenerContainer<String, T> listenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        listenerContainer.setAutoStartup(false);
        // bean name is the prefix of kafka consumer thread name
        listenerContainer.setBeanName("kafka-message-listener");
        return listenerContainer;
    }*/

    /*@Bean
    @ConditionalOnProperty(value = "kafka.notification.topic")
    public Consumer<String, T> createConsumer()
    {
        final Consumer<String, T> consumer = new KafkaConsumer<>(consumerProperties());
        consumer.subscribe(Collections.singletonList(consumerTopic));
        return consumer;
    }*/

    /*@Bean
    //@ConditionalOnProperty(value = "kafka.bootstrap-servers")
    public Map<String, Object> consumerProperties(){
        //Properties props = new Properties();
        Map<String, Object> props = new HashMap<>();
        log.info("bootstrapServers : {}",bootstrapServers);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-kafka");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }*/

    /*@Bean
    public String getTopic()
    {
        var topic = context.getEnvironment().resolvePlaceholders("${kafka.notification.topic}");
        log.info("topic : {}",topic);
        return topic;
    }*/
}
