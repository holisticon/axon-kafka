package org.axonframework.kafka.example.receiver;

import java.util.Map;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.axonframework.config.kafka.KafkaConfigBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@EnableKafka
public class MessagingConfiguration {

  @Value("${kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${kafka.group}")
  private String group;

  @Bean
  public Map<String, Object> consumerConfigs() {
    return KafkaConfigBuilder.defaultConsumer().bootstrapServers(bootstrapServers)
        .withKeyDeserializer(StringDeserializer.class).withValueDeserializer(ByteArrayDeserializer.class).group(group)
        .asMap();
  }

  @Bean
  public ConsumerFactory<String, byte[]> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory() {
    final ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

}
