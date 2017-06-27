package org.axonframework.kafka.example.receiver.query;

import org.apache.kafka.clients.producer.internals.Sender;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.spring.config.EnableAxon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAxon
public class AxonConfiguration {

  @Bean
  public EventStorageEngine engine() {
    return new InMemoryEventStorageEngine();
  }

  @Autowired
  public void initKafkaPublisher(final EventBus eventBus, final Sender sender, final Serializer serializer) {
  }

  @Bean
  public Serializer serializer() {
    return new JacksonSerializer();
  }
}
