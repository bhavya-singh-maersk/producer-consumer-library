package com.maersk.producer.consumer.library.services;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.header.Headers;

import java.io.File;
import java.nio.charset.StandardCharsets;

@Slf4j
public class CustomAvroDeserializer extends KafkaAvroDeserializer {

    @Override
    @SneakyThrows
    public Object deserialize(String topic, Headers headers, byte[] data)
    {
        Schema schema = new Schema.Parser().parse(new File("src/main/avro/EventNotificationsAdapter_ValueSchema.avsc"));
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        GenericRecord payload = datumReader.read(null, decoder);
        log.info("Deserialized payload: {} size:{}", payload, payload.toString().getBytes(StandardCharsets.UTF_8).length);
        return payload;
    }
}
