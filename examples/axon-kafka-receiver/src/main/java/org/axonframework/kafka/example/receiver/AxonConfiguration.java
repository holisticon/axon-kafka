package org.axonframework.kafka.example.receiver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.kafka.example.receiver.query.NotificationLoggingListener;
import org.axonframework.messaging.kafka.DefaultSubscribableEventSource;
import org.axonframework.messaging.kafka.KafkaMessageSource;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.axonframework.spring.config.EnableAxon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableAxon
@Slf4j
public class AxonConfiguration {

  @Bean
  public Serializer serializer() {
    return new XStreamSerializer();
  }

  @Bean
  public DefaultSubscribableEventSource source() {
    return new DefaultSubscribableEventSource() {
      private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArrayList<>();
      @Override
      public List<Consumer<List<? extends EventMessage<?>>>> getEventProcessors() {
        return eventProcessors;
      }
    };
  }

  @Autowired
  public void configure(EventHandlingConfiguration config, DefaultSubscribableEventSource source) {
    final String packageName = NotificationLoggingListener.class.getPackage().getName();
    log.info("Register event processor {} for {}", source.getClass(), packageName);
    config.registerSubscribingEventProcessor(packageName, c -> source);
  }

  @Bean
  public KafkaMessageSource receiver(Serializer serializer, DefaultSubscribableEventSource source) {
    return new KafkaMessageSource(serializer, source) {

      @KafkaListener(topics = "${kafka.event-topic}")
      public void receive(final ConsumerRecord<String, byte[]> record) {
        log.info("Received message of size {}", record.value().length);
        super.receive(record.value());
      }

    };
  }
}
