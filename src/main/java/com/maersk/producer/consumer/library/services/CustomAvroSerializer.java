package com.maersk.producer.consumer.library.services;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Base64;

@Slf4j
@Component
public class CustomAvroSerializer extends KafkaAvroSerializer {

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        Schema schema = ReflectData.get().getSchema(data.getClass());
        log.info("Payload schema: {}", schema.getName());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new ReflectDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer)
                .setCodec(CodecFactory.deflateCodec(9))
                .create(schema, outputStream)) {
            dataFileWriter.append((GenericRecord) data);
        }
        return Base64.getEncoder().encode(outputStream.toByteArray());
    }

   /* @SneakyThrows
    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        Schema schema = ReflectData.get().getSchema(data.getClass());
        log.info("Payload schema: {}", schema.getName());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new ReflectDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write((GenericRecord) data, encoder);
        encoder.flush();
        outputStream.close();
        log.info("compressed avro payload size: {} bytes", outputStream.toByteArray().length);
        return outputStream.toByteArray();
    }*/
}
