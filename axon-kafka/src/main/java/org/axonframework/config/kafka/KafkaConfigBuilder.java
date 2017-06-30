package org.axonframework.config.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Fluent configuration builder.
 * 
 * @author Simon Zambrovski, Holisticon AG
 *
 */
public class KafkaConfigBuilder {

  protected final Properties properties = new Properties();

  public static ProducerConfiguration defaultProducer() {
    return defaultProducer(new Properties());
  }

  public static ProducerConfiguration defaultProducer(final Properties properties) {
    final ProducerConfiguration builder = new ProducerConfiguration();
    builder.withKeySerializer(StringSerializer.class);
    builder.withValueSerializer(StringSerializer.class);

    builder.properties.put("acks", "all");
    builder.properties.put("retries", 0);
    builder.properties.put("batch.size", 16384);
    builder.properties.put("linger.ms", 1);
    builder.properties.put("buffer.memory", 33554432);

    fill(properties, builder.properties);
    
    return builder;
  }

  public static ConsumerConfiguration defaultConsumer() {
    return defaultConsumer(new Properties());
  }

  public static ConsumerConfiguration defaultConsumer(final Properties properties) {
    final ConsumerConfiguration builder = new ConsumerConfiguration();
    builder.withKeyDeserializer(StringDeserializer.class);
    builder.withValueDeserializer(StringDeserializer.class);
    builder.properties.put("enable.auto.commit", "true");
    builder.properties.put("auto.commit.interval.ms", "1000");

    fill(properties, builder.properties);
    
    return builder;
  }

  public KafkaConfigBuilder withProperty(final String propertyName, final String propertyValue) {
    if (propertyValue != null) {
      properties.put(propertyName, propertyValue);
    }
    return this;
  }

  public KafkaConfigBuilder withSystemProperty(final String propertyName, final String systemPropertyName) {
    final String propertyValue = System.getProperty(systemPropertyName);
    if (propertyValue != null) {
      properties.put(propertyName, propertyValue);
    }
    return this;
  }

  public Properties build() {
    return properties;
  }

  public Map<String, Object> asMap() {
    final Map<String, Object> result = new HashMap<String, Object>();
    properties.keySet().stream().forEach(key -> result.put((String) key, properties.get(key)));
    return result;
  }

  public static class ConsumerConfiguration extends KafkaConfigBuilder {
    public ConsumerConfiguration withKeyDeserializer(final Class<? extends Deserializer<?>> clazz) {
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, clazz.getName());
      return this;
    }

    public ConsumerConfiguration withValueDeserializer(final Class<? extends Deserializer<?>> clazz) {
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz.getName());
      return this;
    }

    public ConsumerConfiguration bootstrapServers(final String bootstrapServers) {
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    public ConsumerConfiguration group(final String group) {
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
      return this;
    }

  }

  public static class ProducerConfiguration extends KafkaConfigBuilder {

    public ProducerConfiguration withKeySerializer(final Class<? extends Serializer<?>> clazz) {
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, clazz.getName());
      return this;
    }

    public ProducerConfiguration withValueSerializer(final Class<? extends Serializer<?>> clazz) {
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, clazz.getName());
      return this;
    }

    public ProducerConfiguration bootstrapServers(final String bootstrapServers) {
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }
  }

  private static void fill(final Properties source, final Properties target) {
    if (source != null && !source.isEmpty() && target != null) {
      source.forEach((key, value) -> target.put(key, value));
    }

  }

}
