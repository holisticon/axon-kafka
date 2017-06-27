package org.axonframework.kafka.example.receiver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AxonKafkaReceiverApplication {

  public static void main(String[] args) {
    SpringApplication.run(AxonKafkaReceiverApplication.class, args);
  }

}
