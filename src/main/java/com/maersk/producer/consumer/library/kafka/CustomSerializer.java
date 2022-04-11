package com.maersk.producer.consumer.library.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.util.Map;

public class CustomSerializer <T extends Serializable> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, T t) {
        return SerializationUtils.serialize(t);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        //return Serializer.super.serialize(topic, headers, data);
        return null;
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
