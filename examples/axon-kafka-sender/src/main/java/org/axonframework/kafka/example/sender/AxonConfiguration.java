package org.axonframework.kafka.example.sender;

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.kafka.Sender;
import org.axonframework.kafka.message.KafkaMessageConverter;
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
    final KafkaMessageConverter converter = new KafkaMessageConverter(serializer);
    eventBus.subscribe(events -> {
      events.stream().map(event -> converter.createKafkaMessage(event))
          .forEach(kafkaMessage -> sender.send(kafkaMessage));
    });
  }

  @Bean
  public Serializer serializer() {
    return new JacksonSerializer();
  }
}
