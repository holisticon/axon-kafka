package org.axonframework.kafka.example.sender;

import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.config.kafka.KafkaConfigBuilder;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore;
import org.axonframework.eventhandling.tokenstore.kafka.KafkaTokenStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.kafka.KafkaEventStoreEngine;
import org.axonframework.messaging.kafka.Sender;
import org.axonframework.messaging.kafka.message.KafkaMessage;
import org.axonframework.messaging.kafka.message.KafkaMessageConverter;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;

import com.google.common.base.Predicates;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
@Slf4j
@ToString
public class SenderConfiguration {

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
                .withValueDeserializer(ByteArrayDeserializer.class).group(UUID.randomUUID().toString()).build();
    }

    @Bean
    public EventStorageEngine engine(Serializer serializer) {
        log.info("Configuring store engine.");
        return new KafkaEventStoreEngine(serializer, null, null, eventStorageTopic, bootstrapServers);
    }

    @Bean
    public TokenStore tokenStore(EventStorageEngine engine) {
        if (engine instanceof KafkaEventStoreEngine) {
            log.info("Configuring Kafka token store.");
            return new KafkaTokenStore(((KafkaEventStoreEngine) engine).getConsumer(), eventStorageTopic);
        }
        return new InMemoryTokenStore();
    }

    // @Bean
    public Sender sender() {
        return new Sender() {
            @Override
            public void send(KafkaMessage kafkaMessage) {
                final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfigs());
                try {
                    final Future<RecordMetadata> future = producer
                            .send(new ProducerRecord<>(eventMessagingTopic, kafkaMessage.getKey(), kafkaMessage.getPayload()));
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

    // @Bean
    public EventStorageEngine engine() {
        return new InMemoryEventStorageEngine();
    }

    // @Autowired
    public void initKafkaPublisher(final EventBus eventBus, final Sender sender, final Serializer serializer) {
        final KafkaMessageConverter converter = new KafkaMessageConverter(serializer);
        eventBus.subscribe(events -> {
            events.stream().map(event -> converter.createKafkaMessage(event)).forEach(kafkaMessage -> sender.send(kafkaMessage));
        });
    }

    @Autowired
    public void configure(EventHandlingConfiguration config) {
        log.info("Configured tracking processors");
        config.usingTrackingProcessors();
    }

    @Bean
    public Serializer serializer() {
        return new XStreamSerializer();
    }

    @Bean
    public Docket newsApi() {
      // @formatter:off
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                  .apis(RequestHandlerSelectors.any())
                  .paths(Predicates.not(PathSelectors.regex("/error.*")))
                  .build()
                .pathMapping("/")
                .apiInfo(new ApiInfoBuilder().title("Kafka Sender API").build())
                .directModelSubstitute(LocalDate.class,
                    String.class)
                .genericModelSubstitutes(ResponseEntity.class)
                .enableUrlTemplating(true) 
                ;
        // @formatter:on
    }

}
