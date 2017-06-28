package org.axonframework.kafka.example.sender;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.kafka.Sender;
import org.axonframework.kafka.config.KafkaConfigBuilder;
import org.axonframework.kafka.message.KafkaMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class MessagingConfiguration {

  @Value("${kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${kafka.event-topic}")
  private String eventTopic;

  @Value("${kafka.timeout:1000}")
  private Long timeout;

  @Bean
  public Properties producerConfigs() {
    return KafkaConfigBuilder.defaultProducer().bootstrapServers(bootstrapServers)
        .withKeySerializer(StringSerializer.class).withValueSerializer(ByteArraySerializer.class).build();
  }

  @Bean
  public Sender sender() {
    return new Sender() {
      @Override
      public void send(KafkaMessage kafkaMessage) {
        final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfigs());
        try {
          final Future<RecordMetadata> future = producer
              .send(new ProducerRecord<>(eventTopic, kafkaMessage.getKey(), kafkaMessage.getPayload()));
          RecordMetadata recordMetadata = future.get(timeout, TimeUnit.MILLISECONDS);
          log.trace("Message with offset {} sent.", recordMetadata.offset());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          log.error("Error sending message to kafka topic", e);
        } finally {
          producer.close();
        }
      }
    };
  }
}
