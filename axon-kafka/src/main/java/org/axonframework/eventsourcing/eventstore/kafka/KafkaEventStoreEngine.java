package org.axonframework.eventsourcing.eventstore.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.Assert;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.config.kafka.KafkaConfigBuilder;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventStoreEngine extends AbstractEventStorageEngine {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventStoreEngine.class);

    private static final long TIMEOUT = 1000;

    private KafkaProducer<String, byte[]> producer;
    private KafkaConsumer<String, byte[]> consumer;

    private final String eventStorage;
    private final String bootstrapServers;

    public KafkaEventStoreEngine(final Serializer serializer, final EventUpcaster upcasterChain,
            final PersistenceExceptionResolver persistenceExceptionResolver, final String eventStorage, final String bootstrapServers) {
        super(serializer, upcasterChain, persistenceExceptionResolver);
        this.eventStorage = eventStorage;
        this.bootstrapServers = bootstrapServers;
        init();
    }

    private void init() {

        final String groupId = "storage-" + UUID.randomUUID().toString();
        // setup consumer
        final Properties consumerProps = KafkaConfigBuilder.defaultConsumer().withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class).bootstrapServers(bootstrapServers).group(groupId).build();
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(eventStorage));

        // setup producer
        final Properties producerProps = KafkaConfigBuilder.defaultProducer().withKeySerializer(StringSerializer.class)
                .withValueSerializer(ByteArraySerializer.class).bootstrapServers(bootstrapServers).build();
        producer = new KafkaProducer<>(producerProps);

    }

    @Override
    protected void appendEvents(final List<? extends EventMessage<?>> events, final Serializer serializer) {
        log.info("appendEvents {}", events);
        if (events.isEmpty()) {
            return;
        }
        events.stream().map(event -> KafkaStorageConverter.toRecord(event, serializer, eventStorage))
        .forEach(record -> producer.send(record, (metadata, exception) -> {
            if (metadata != null) {
                log.info("Completed event append for {}", metadata.offset());
            } else {
                log.error("Exception during event append", exception);
                handlePersistenceException(exception, events.get(0));
            }
        }));
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readEventData(final String identifier, final long firstSequenceNumber) {
        log.info("readEventData {} {}", identifier, firstSequenceNumber);
        // locate to beginning
        consumer.seekToBeginning(Collections.emptyList());

        return StreamSupport
                .stream(consumer.poll(TIMEOUT).records(this.eventStorage).spliterator(), false)
                .map(record -> KafkaStorageConverter.createDomainEventEntry(record, getSerializer()));
    }

    @Override
    protected Stream<? extends TrackedEventData<?>> readEventData(final TrackingToken trackingToken, final boolean mayBlock) {
        log.info("readEventData {} {}", trackingToken, mayBlock);

        Assert.isTrue(trackingToken == null || trackingToken instanceof GlobalSequenceTrackingToken,
                () -> String.format("Token [%s] is of the wrong type. Expected [%s]", trackingToken, GlobalSequenceTrackingToken.class.getSimpleName()));

        final long offset = ((GlobalSequenceTrackingToken) trackingToken).getGlobalIndex();
        consumer.assignment().stream().forEach(partition -> consumer.seek(partition, offset));
        return StreamSupport
                .stream(consumer.poll(TIMEOUT).records(this.eventStorage).spliterator(), false)
                .map(record -> KafkaStorageConverter.createTrackedEventEntry(record, getSerializer()));
    }

    @Override
    protected Optional<? extends DomainEventData<?>> readSnapshotData(final String aggregateIdentifier) {
        log.info("readSnapshotData {}", aggregateIdentifier);
        return Optional.empty();
    }

    @Override
    protected void storeSnapshot(final DomainEventMessage<?> snapshot, final Serializer serializer) {
        log.info("storeSnapshot {}", snapshot);
    }

    public KafkaConsumer<String, byte[]> getConsumer() {
        return consumer;
    }

}
