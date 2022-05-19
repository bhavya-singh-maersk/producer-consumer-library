package com.maersk.producer.consumer.library.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class StorageConnectionFactory {

    @Autowired
    @Qualifier("azure-blob")
    StorageService azureBlobStorageService;

    public StorageService getStorageService(String storageType)
    {
        switch (storageType) {
            case "azure-blob": {
                return azureBlobStorageService;
            }
            default:
                throw new RuntimeException("No storage account found");
        }
    }
}
