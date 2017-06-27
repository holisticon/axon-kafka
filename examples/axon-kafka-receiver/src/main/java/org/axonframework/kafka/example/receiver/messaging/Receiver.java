package org.axonframework.kafka.example.receiver.messaging;

import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Receiver {

  @KafkaListener(topics = "${kafka.event-topic}")
  public void receive(final String message) {
    log.info("received message='{}'", message);
  }
}
