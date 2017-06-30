package org.axonframework.kafka.example.sender;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.ToString;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.config.kafka.KafkaConfigBuilder;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.kafka.KafkaTokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.kafka.KafkaEventStoreEngine;
import org.axonframework.messaging.kafka.Sender;
import org.axonframework.messaging.kafka.message.KafkaMessage;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;

@Configuration
@Slf4j
@ToString
public class MessagingConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.event-messaging}")
    private String eventMessagingTopic;

    @Value("${kafka.event-storage}")
    private String eventStorageTopic;

    @Value("${kafka.timeout:1000}")
    private Long timeout;

    @PostConstruct
    void log() {
        log.info(this.toString());
    }

    @Bean
    public Properties producerConfigs() {
        return KafkaConfigBuilder.defaultProducer().bootstrapServers(bootstrapServers).withKeySerializer(StringSerializer.class)
                .withValueSerializer(ByteArraySerializer.class).build();
    }

    @Bean
    public Properties consumerConfigs() {
        return KafkaConfigBuilder.defaultConsumer().bootstrapServers(bootstrapServers).withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class).build();
    }
    
    @Bean
    public EventStorageEngine engine(Serializer serializer) {
        //return new KafkaEventStoreEngine(serializer, null, null, eventStorageTopic, bootstrapServers);
        return new InMemoryEventStorageEngine();
    }

//    //@Bean
//    public TokenStore tokenStore() {
//        log.info("Configured token store");
//        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfigs());
//        return new KafkaTokenStore(consumer, eventStorageTopic);
//    }
//
    @Bean
    public Sender sender() {
        return new Sender() {
            @Override
            public void send(KafkaMessage kafkaMessage) {
                final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfigs());
                try {
                    final Future<RecordMetadata> future = producer.send(new ProducerRecord<>(eventMessagingTopic, kafkaMessage.getKey(), kafkaMessage.getPayload()));
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
