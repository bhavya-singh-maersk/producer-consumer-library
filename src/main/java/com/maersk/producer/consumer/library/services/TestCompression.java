package com.maersk.producer.consumer.library.services;


import lombok.extern.slf4j.Slf4j;
import net.apmoller.ohm.adapter.avro.model.EventNotificationsAdapterModel;
import org.apereo.cas.util.CompressionUtils;
import org.json.JSONObject;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@Slf4j
public class TestCompression {

    public static void testJsonCompression()
    {
        String originalPayload = "{\n" +
                "\t\"db_response\": {\n" +
                "\t\t\"container\": {\n" +
                "\t\t\t\"archive\": {\n" +
                "\t\t\t\t\"doctype\": \"\",\n" +
                "\t\t\t\t\"expirydate\": \"2022-10-01\",\n" +
                "\t\t\t\t\"index_s\": \"43e4be58-9c54-493f-ad40-6847bad741a5\",\n" +
                "\t\t\t\t\"code\": \"0A732E776A9C61AEEF6A802CD9F7\",\n" +
                "\t\t\t\t\"index_m\": \"43e4be58-9c54-493f-ad40-6847bad741a5\",\n" +
                "\t\t\t\t\"docid\": 500005964,\n" +
                "\t\t\t\t\"domain\": \"DKGIS\",\n" +
                "\t\t\t\t\"save\": true\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"response\": {\n" +
                "\t\t\t\"returncode\": 0,\n" +
                "\t\t\t\"origin\": \"DMS:SCRBDBKDK007206\",\n" +
                "\t\t\t\"error\": 0,\n" +
                "\t\t\t\"returnstring\": [\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"Docengine\",\n" +
                "\t\t\t\t\t\"content\": \"Added ply [1] by index\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"Docengine\",\n" +
                "\t\t\t\t\t\"content\": \"Could not find ply [2] by index\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t{\n" +
                "\t\t\t\t\t\"source\": \"DBDatabase\",\n" +
                "\t\t\t\t\t\"content\": \"Key_Add\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t]\n" +
                "\t\t},\n" +
                "\t\t\"type\": \"db_extract_package\",\n" +
                "\t\t\"version\": 2,\n" +
                "\t\t\"revision\": 0\n" +
                "\t}\n" +
                "}";
        StringBuilder sb = new StringBuilder();
        sb.append(originalPayload.repeat(10));
        log.info("Original payload size: {} bytes", sb.toString().getBytes(StandardCharsets.UTF_8).length);
        var compressedPayload = CompressionUtils.compress(sb.toString());
        log.info("Compressed payload size: {} bytes", compressedPayload.getBytes(StandardCharsets.UTF_8).length);
        var decompressedPayload = CompressionUtils.decompress(compressedPayload);
        log.info("Decompressed payload: {} size: {} bytes",decompressedPayload, decompressedPayload.getBytes(StandardCharsets.UTF_8).length);
    }

    public static void testAvroCompression()
    {
        CustomAvroSerializer avroSerializer = new CustomAvroSerializer();
        CustomAvroDeserializer avroDeserializer = new CustomAvroDeserializer();
        String testPayload = "{\n" +
                "    \"response\": \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?> <db_response type=\\\"db_extract_package\\\" version=\\\"2\\\" revision=\\\"0\\\"> <response error=\\\"0\\\" returncode=\\\"0\\\" origin=\\\"DMS:SCRBDBKDK007206\\\"> <returnstring source=\\\"Docengine\\\">Added ply [1] by index</returnstring> </response> <container> <archive save=\\\"true\\\" doctype=\\\"\\\" docid=\\\"RNKT00003\\\" expirydate=\\\"2022-10-04\\\"><domain>WCAIND</domain><code>0A732E774E34615B25315973EA2C</code><index_s>2ab97fda-55dd-4ae3-a379-88f45e0a3b37</index_s><index_m>43e4be58-9c54-493f-ad40-6847bad741a5</index_m></archive></container></db_response>\"\n" +
                "}";
        JSONObject response = new JSONObject(testPayload);
        EventNotificationsAdapterModel avro= EventNotificationsAdapterModel.newBuilder()
                .setResponse(response.get("response").toString())
                .setCorrelationId("corId").setMessageType("xml").setMessageId("dfdf-dfd")
                .setSourceSystem("docbroker").setResponseConsumers(Collections.emptyList()).build();
        log.info("Original avro payload size: {} bytes", avro.toString().getBytes(StandardCharsets.UTF_8).length);
        var byteArray = avroSerializer.serialize(null, null, avro);
        avroDeserializer.deserialize(null, null, byteArray);
    }

    public static void main(String[] args) {
        //testJsonCompression();
        testAvroCompression();
    }



}
