package org.axonframework.kafka.example.sender;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.kafka.Sender;
import org.axonframework.kafka.example.sender.messaging.KafkaFakeSender;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class MessagingConfiguration {

    // list of host:port pairs used for establishing the initial connections to
    // the Kakfa cluster
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.event-topic}")
    private String eventTopic;

    @Bean
    public Map<String, Object> producerConfigs() {
	Map<String, Object> props = new HashMap<>();
	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

	return props;
    }

    @Bean
    public ProducerFactory<String, byte[]> producerFactory() {
	return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, byte[]> kafkaTemplate() {
	return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public Sender sender(KafkaTemplate<String, byte[]> template) {
	return new KafkaFakeSender(this.eventTopic);
	// return new KafkaSender(this.eventTopic, template);
    }
}
