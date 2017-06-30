package org.axonframework.kafka.example.sender;

import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.kafka.Sender;
import org.axonframework.messaging.kafka.message.KafkaMessageConverter;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class AxonConfiguration {

//    @Bean
//    public EventStorageEngine engine() {
//        return new InMemoryEventStorageEngine();
//    }

        

    
    @Autowired
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

}
