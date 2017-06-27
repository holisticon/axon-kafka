package org.axonframework.kafka.example.receiver;

import org.axonframework.spring.config.AxonConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({ AxonConfiguration.class, MessagingConfiguration.class })
public class AxonKafkaReceiverApplication {

  public static void main(String[] args) {
    SpringApplication.run(AxonKafkaReceiverApplication.class, args);
  }
}
