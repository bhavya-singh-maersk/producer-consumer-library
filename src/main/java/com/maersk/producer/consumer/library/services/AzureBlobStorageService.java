package com.maersk.producer.consumer.library.services;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service("azure-blob")
public class AzureBlobStorageService<T> implements StorageService<T>{

    private static final String PAYLOAD_FILE_NAME = "${events-payload.file-name}";
    private static final String AZURE_STORAGE_ACCOUNT_NAME = "${azure.storage.account-name}";
    private static final String AZURE_STORAGE_ACCOUNT_KEY = "${azure.storage.account-key}";
    private static final String AZURE_STORAGE_CONTAINER_NAME = "${azure.storage.container-name}";
    private static final String AZURE_STORAGE_ENDPOINT_SUFFIX = "${azure.storage.endpoint-suffix}";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s";

    @Autowired
    private ApplicationContext context;

    @Override
    public String storePayloadToCloud(T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
        var containerDest = getCloudBlobContainer();
        var blobUri = writePayloadFileToBlob(payload, containerDest);
        return blobUri.toString();
    }

    @Override
    public String downloadBlob() {
        BlobClient blobClient = new BlobClientBuilder()
                .endpoint("https://claimscheckpayloads.blob.core.windows.net")
                .sasToken("8OD1vjEqe7ux9m7UiBWImDMfmsWfu52rjkdsHoQ6gUrDyNThUt9U7UDeeEZWssp/IaVWv/CTXv8spi0Kug9gpA==")
                .containerName("producer")
                .blobName("docbroker_response_1781d610-ba3f-4304-a90f-c693687ac8d2.dat")
                .buildClient();
        log.info("blob client: {}", blobClient.getBlobName());
        BinaryData content = blobClient.downloadContent();
        log.info("Downloaded blob content: {}", content.toString());
        return content.toString();
    }

    public URI writePayloadFileToBlob(T payload, CloudBlobContainer containerDest) throws URISyntaxException, StorageException, IOException {
        CloudBlockBlob cloudBlockBlob = containerDest.getBlockBlobReference(getPayloadFilename());
        try (BlobOutputStream bos = cloudBlockBlob.openOutputStream()) {
            byte[] byteArray = SerializationUtils.serialize((Serializable) payload);
            bos.write(byteArray);
        }
        return cloudBlockBlob.getUri();
    }

    @Override
    public T readPayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException {
        var containerDest = getCloudBlobContainer();
        T payload = readPayloadFileFromBlob(new URI(blobReference), containerDest);
        if (Objects.nonNull(payload))
        {
            Class<?> clazz = payload.getClass();
            log.info("Payload class name : {}", clazz.getName());
        }
        return payload;
    }

    @Override
    public T getPayloadFromBlob(T payloadReference, String isLargePayload) throws URISyntaxException, InvalidKeyException, StorageException {
        if (isLargePayload.equals("NO"))
        {
            return payloadReference;
        }
        var containerDest = getCloudBlobContainer();
        JSONObject jsonObject = new JSONObject(payloadReference.toString());
        log.info("jsonObject: {}", jsonObject);
        String referenceLink = null;
        if (jsonObject.has("reference"))
        {
            referenceLink = (String) jsonObject.get("reference");
        }
        return readPayloadFileFromBlob(new URI(referenceLink), containerDest);
    }

    private T readPayloadFileFromBlob(URI uri, CloudBlobContainer containerDest) throws StorageException, URISyntaxException {
        T payload = null;
        CloudBlockBlob cloudBlockBlob = containerDest.getBlockBlobReference(new CloudBlockBlob(uri).getName());
        log.info(cloudBlockBlob.getName());
        try (BlobInputStream bis = cloudBlockBlob.openInputStream())
        {
            byte[] byteArray = bis.readAllBytes();
            payload = SerializationUtils.deserialize(byteArray);
            log.info("Payload after deserialization: {}", payload);
        } catch (IOException io) {
            log.error("Exception while reading payload from blob", io);
        }
        return payload;
    }

    public CloudBlobContainer getCloudBlobContainer() throws StorageException, URISyntaxException, InvalidKeyException {
        String containerName = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_CONTAINER_NAME);
        log.info("containerName: {}", containerName);
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("10.255.237.21", 8887));
        OperationContext op = new OperationContext();
        op.setProxy(proxy);
        CloudStorageAccount storageAccountDest = CloudStorageAccount.parse(getConnectionString());
        CloudBlobClient blobClientDest = storageAccountDest.createCloudBlobClient();
        CloudBlobContainer containerDest = blobClientDest.getContainerReference(containerName);
        log.info("Container: {}", containerDest.getName());
        return containerDest;
    }

    private String getConnectionString()
    {
        String storageAccountName = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ACCOUNT_NAME);
        String storageAccountKey = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ACCOUNT_KEY);
        String endpointSuffix = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ENDPOINT_SUFFIX);
        String connectionString = String.format(CONNECTION_STRING,storageAccountName,storageAccountKey,endpointSuffix);
        log.info("Azure storage connection string: {}", connectionString);
        return connectionString;
    }

    public String getPayloadFilename()
    {
        String fileName = context.getEnvironment().resolvePlaceholders(PAYLOAD_FILE_NAME);
        fileName = fileName.concat("_").concat(UUID.randomUUID().toString()).concat(".dat");
        log.info("Payload file name: {}", fileName);
        return fileName;
    }
}
