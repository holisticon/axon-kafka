package org.axonframework.eventsourcing.eventstore.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.Assert;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.config.kafka.KafkaConfigBuilder;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GapAwareTrackingToken;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.messaging.kafka.message.KafkaMessage;
import org.axonframework.messaging.kafka.message.KafkaMessageConverter;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventStoreEngine extends AbstractEventStorageEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventStoreEngine.class);

    private static final long TIMEOUT = 1000;

    private KafkaProducer<String, byte[]> producer;
    private KafkaConsumer<String, byte[]> consumer;

    private String eventStorage;
    private String bootstrapServers;

    public KafkaEventStoreEngine(Serializer serializer, EventUpcaster upcasterChain, PersistenceExceptionResolver persistenceExceptionResolver,
            String eventStorage, String bootstrapServers) {
        super(serializer, upcasterChain, persistenceExceptionResolver);
        this.eventStorage = eventStorage;
        this.bootstrapServers = bootstrapServers;

        init();
    }

    private void init() {

        // setup consumer
        final Properties consumerProps = KafkaConfigBuilder.defaultConsumer().withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class).bootstrapServers(bootstrapServers).build();
        consumer = new KafkaConsumer<>(consumerProps);

        // setup producer
        final Properties producerProps = KafkaConfigBuilder.defaultProducer().withKeySerializer(StringSerializer.class)
                .withValueSerializer(ByteArraySerializer.class).bootstrapServers(bootstrapServers).build();
        producer = new KafkaProducer<>(producerProps);

    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        LOGGER.info("appendEvents {}", events);
        if (events.isEmpty()) {
            return;
        }
        events.stream().map(event -> createKafkaMessage(serializer, event)).forEach(kafkaMessage -> producer
                .send(new ProducerRecord<String, byte[]>(eventStorage, kafkaMessage.getKey(), kafkaMessage.getPayload()), new Callback() {
                    @Override
                    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                        if (metadata != null) {
                            LOGGER.info("Completed event append for {}", metadata.offset());
                        } else {
                            LOGGER.error("Exception during event append", exception);
                            handlePersistenceException(exception, events.get(0));
                        }
                    }
                }));
    }

    private KafkaMessage createKafkaMessage(Serializer serializer, EventMessage<?> event) {
        return KafkaMessageConverter.createKafkaMessage(serializer, event);
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readEventData(String identifier, long firstSequenceNumber) {
        LOGGER.info("readEventData {} {}", identifier, firstSequenceNumber);
        // locate to beginning
        consumer.seekToBeginning(Collections.emptyList());

        // return Collections.<DomainEventData<?>> emptyList().stream();
        return StreamSupport.stream(consumer.poll(TIMEOUT).records(this.eventStorage).spliterator(), false).map(record -> createDomainEvent(record));
    }


    @Override
    protected Stream<? extends TrackedEventData<?>> readEventData(TrackingToken trackingToken, boolean mayBlock) {
        LOGGER.info("readEventData {} {}", trackingToken, mayBlock);
        
        Assert.isTrue(trackingToken == null || trackingToken instanceof GlobalSequenceTrackingToken,
                () -> String.format("Token [%s] is of the wrong type. Expected [%s]", trackingToken, GlobalSequenceTrackingToken.class.getSimpleName()));

        final long offset = ((GlobalSequenceTrackingToken) trackingToken).getGlobalIndex();
        consumer.assignment().stream().forEach(partition -> consumer.seek(partition, offset));
        return StreamSupport.stream(consumer.poll(TIMEOUT).records(this.eventStorage).spliterator(), false).map(record -> createTrackedEvent(record));        
    }


    @Override
    protected Optional<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        LOGGER.info("readSnapshotData {}", aggregateIdentifier);
        return Optional.empty();
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        LOGGER.info("storeSnapshot {}", snapshot);
    }

    public KafkaConsumer<String, byte[]> getConsumer() {
        return consumer;
    }

}
