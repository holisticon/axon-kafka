package org.axonframework.kafka.example.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AxonKafkaSenderApplication {

    public static void main(String[] args) {
        SpringApplication.run(AxonKafkaSenderApplication.class, args);
    }
}
