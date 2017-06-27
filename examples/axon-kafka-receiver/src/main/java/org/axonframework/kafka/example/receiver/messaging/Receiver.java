package org.axonframework.kafka.example.receiver.messaging;

import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.kafka.message.KafkaMessageConverter;
import org.axonframework.serialization.Serializer;
import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Receiver {

  private KafkaMessageConverter converter;
  private SubscribabaleSource source;

  public Receiver(Serializer serializer, SubscribabaleSource source) {
    this.source = source;
    this.converter = new KafkaMessageConverter(serializer);
  }

  @KafkaListener(topics = "${kafka.event-topic}")
  public void receive(final ConsumerRecord<String, byte[]> record) {
    log.info("Received message of size {}", record.value().length);
    if (!source.getEventProcessors().isEmpty()) {
      final Optional<EventMessage<?>> createAxonEventMessage = converter.createAxonEventMessage(record.value());
      if (!createAxonEventMessage.isPresent()) {
        log.warn("Unable to read message. Ignoring it.");
      } else {
        log.info("Distributing the message to {} eventProcessors", source.getEventProcessors().size());
        source.getEventProcessors().forEach(ep -> ep.accept(Collections.singletonList(createAxonEventMessage.get())));
      }
    } else {
      log.warn("No event processors found.");
    }
  }

}
