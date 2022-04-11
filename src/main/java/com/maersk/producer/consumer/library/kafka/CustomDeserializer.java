package com.maersk.producer.consumer.library.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class CustomDeserializer <T extends Serializable> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return Objects.nonNull(bytes) ? (T) SerializationUtils.deserialize(bytes) : null;
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
       // return Deserializer.super.deserialize(topic, headers, data);
       // return Objects.nonNull(data) ? (T) SerializationUtils.deserialize(data) : null;
        return null;
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
