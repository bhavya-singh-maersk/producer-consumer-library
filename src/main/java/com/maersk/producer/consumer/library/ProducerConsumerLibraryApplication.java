package com.maersk.producer.consumer.library;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@EnableRetry
@SpringBootApplication
public class ProducerConsumerLibraryApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerConsumerLibraryApplication.class, args);
	}

}
