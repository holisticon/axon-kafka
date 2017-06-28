package org.axonframework.kafka.example.receiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.kafka.KafkaMessageSource;
import org.axonframework.kafka.SubscribableEventSource;
import org.axonframework.kafka.example.receiver.messaging.SubscribabaleSource;
import org.axonframework.kafka.example.receiver.query.NotificationLoggingListener;
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
  public SubscribabaleSource source() {
    log.info("Create subscribable event source");
    return new SubscribabaleSource();
  }

  @Autowired
  public void configure(EventHandlingConfiguration config, SubscribabaleSource source) {
    final String packageName = NotificationLoggingListener.class.getPackage().getName();
    log.info("Register event processor {} for {}", source.getClass(), packageName);
    config.registerSubscribingEventProcessor(packageName, c -> source);
  }

  @Bean
  public KafkaMessageSource receiver(Serializer serializer, SubscribableEventSource source) {
    return new KafkaMessageSource(serializer, source) {

      @KafkaListener(topics = "${kafka.event-topic}")
      public void receive(final ConsumerRecord<String, byte[]> record) {
        log.info("Received message of size {}", record.value().length);
        super.receive(record.value());
      }

    };
  }
}
