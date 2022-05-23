package com.maersk.producer.consumer.library.services;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apereo.cas.util.CompressionUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class TestCompression {

    public static void testCompression()
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

    public static void main(String[] args) throws IOException, ParseException {
        testCompression();
    }



}
