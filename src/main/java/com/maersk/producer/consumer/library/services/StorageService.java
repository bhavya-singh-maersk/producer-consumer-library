package com.maersk.producer.consumer.library.services;

import com.microsoft.azure.storage.StorageException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public interface StorageService<T> {

    String storePayloadToCloud(T payload) throws URISyntaxException, InvalidKeyException, StorageException, IOException;

    String downloadBlob();

    T readPayloadFromBlob(String blobReference) throws URISyntaxException, InvalidKeyException, StorageException;

    T getPayloadFromBlob(T payloadReference, String isLargePayload) throws URISyntaxException, InvalidKeyException, StorageException;
}
