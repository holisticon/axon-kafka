package org.axonframework.kafka.example.receiver.query;

import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.kafka.example.receiver.messaging.Receiver;
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
  public void initKafkaReceiver(EventHandlingConfiguration config, Receiver receiver) {
    config.registerSubscribingEventProcessor(NotificationLoggingListener.class.getPackage().getName(), c -> receiver);
  }
}
