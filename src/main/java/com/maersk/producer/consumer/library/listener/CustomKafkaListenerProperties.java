package com.maersk.producer.consumer.library.listener;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "custom.kafka")
//@Component
public class CustomKafkaListenerProperties {
    private Map<String, CustomKafkaListenerProperty> listeners;

}
