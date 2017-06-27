package org.axonframework.kafka.example.receiver.messaging;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.kafka.message.KafkaMessageConverter;
import org.axonframework.messaging.SubscribableMessageSource;
import org.axonframework.serialization.Serializer;
import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Receiver implements SubscribableMessageSource<EventMessage<?>> {

  private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArrayList<>();

  private KafkaMessageConverter converter;
  
  public Receiver(Serializer serializer) {
    converter = new KafkaMessageConverter(serializer);
  }

  @KafkaListener(topics = "${kafka.event-topic}")
  public void receive(final byte[] record) {
    log.info("Received message of size {}", record.length);
    if (!eventProcessors.isEmpty()) {
      final Optional<EventMessage<?>> createAxonEventMessage = converter.createAxonEventMessage(record);
      if (!createAxonEventMessage.isPresent()) {
        log.warn("Unable to read message. Ignoring it.");
      } else {
        log.info("Distributing the message to {} eventProcessors", eventProcessors.size());
        eventProcessors.forEach(ep -> ep.accept(Collections.singletonList(createAxonEventMessage.get())));
      }
    } else {
      log.warn("No event processors found.");
    }
  }
  
  @Override
  public Registration subscribe(final Consumer<List<? extends EventMessage<?>>> messageProcessor) {
    eventProcessors.add(messageProcessor);
    return () -> eventProcessors.remove(messageProcessor);
  }

}
