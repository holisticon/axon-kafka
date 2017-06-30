package org.axonframework.eventsourcing.eventstore.kafka;

import static org.axonframework.serialization.MessageSerializer.serializePayload;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GenericDomainEventEntry;
import org.axonframework.eventsourcing.eventstore.GenericTrackedDomainEventEntry;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.kafka.message.KafkaMessage.KafkaPayload;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStorageConverter {

    private static final Logger log = LoggerFactory.getLogger(KafkaStorageConverter.class);

    public static ProducerRecord<String, byte[]> toRecord(final EventMessage<?> message, final Serializer serializer, final String eventStorage) {

        final SerializedObject<byte[]> serializedObject = serializePayload(message, serializer, byte[].class);
        final Map<String, Object> headers = new HashMap<>();
        message.getMetaData().forEach((k, v) -> headers.put("axon-metadata-" + k, v));
        headers.put("axon-message-id", message.getIdentifier());
        headers.put("axon-message-type", serializedObject.getType().getName());
        headers.put("axon-message-revision", serializedObject.getType().getRevision());
        headers.put("axon-message-timestamp", message.getTimestamp().toString());
        if (message instanceof DomainEventMessage<?>) {
            headers.put("axon-message-aggregate-id", ((DomainEventMessage<?>) message).getAggregateIdentifier());
            headers.put("axon-message-aggregate-seq", ((DomainEventMessage<?>) message).getSequenceNumber());
            headers.put("axon-message-aggregate-type", ((DomainEventMessage<?>) message).getType());
        }

        final KafkaPayload payload = new KafkaPayload(headers, serializedObject.getData());
        final SerializedObject<byte[]> serializedKafkaPayload = serializer.serialize(payload, byte[].class);

        return new ProducerRecord<>(eventStorage, serializedKafkaPayload.getData());
    }

    private static GenericTrackedDomainEventEntry<byte[]> toEntry(final ConsumerRecord<String, byte[]> record, final Serializer serializer) {
        final SimpleSerializedObject<byte[]> serializedKafkaMessage = new SimpleSerializedObject<>(record.value(), byte[].class, byte[].class.getName(), null);
        final KafkaPayload kafkaPayload = serializer.deserialize(serializedKafkaMessage);
        log.trace("Converting kafka payload {}", kafkaPayload);

        final Map<String, Object> headers = kafkaPayload.getHeaders();
        if (!headers.keySet().containsAll(Arrays.asList("axon-message-id", "axon-message-type"))) {
            return null;
        }

        final Map<String, Object> metaData = new HashMap<>();
        headers.forEach((k, v) -> {
            if (k.startsWith("axon-metadata-")) {
                metaData.put(k.substring("axon-metadata-".length()), v);
            }
        });

        final String payloadRevision = Objects.toString(headers.get("axon-message-revision"), null);
        final String payloadType = Objects.toString(headers.get("axon-message-type"));
        final String timestamp = Objects.toString(headers.get("axon-message-timestamp"));
        final Instant instant = Instant.parse(timestamp);

        final String aggregateIdentifier = Objects.toString(headers.get("axon-message-aggregate-id"));
        final long sequenceNumber = (Long) headers.get("axon-message-aggregate-seq");
        final String eventIdentifier = Objects.toString(headers.get("axon-message-id"));

        final String type = Objects.toString(headers.get("axon-message-aggregate-type"));
        final TrackingToken token = new GlobalSequenceTrackingToken(record.offset());

        final byte[] entryMetadata = serializer.serialize(MetaData.from(metaData), byte[].class).getData();

        return new GenericTrackedDomainEventEntry<>(token, type, aggregateIdentifier, sequenceNumber, eventIdentifier, instant, payloadType, payloadRevision,
                kafkaPayload.getPayload(), entryMetadata);
    }

    public static TrackedEventData<?> createTrackedEventEntry(final ConsumerRecord<String, byte[]> record, final Serializer serializer) {
        return toEntry(record, serializer);
    }

    public static DomainEventData<?> createDomainEventEntry(final ConsumerRecord<String, byte[]> record, final Serializer serializer) {
        final GenericTrackedDomainEventEntry<byte[]> entry = toEntry(record, serializer);
        final byte[] payload = ((SimpleSerializedObject<byte[]>) entry.getPayload()).getData();
        final byte[] metaData = ((SerializedMetaData<byte[]>) entry.getMetaData()).getData();
        return new GenericDomainEventEntry<>(entry.getType(), entry.getAggregateIdentifier(), entry.getSequenceNumber(), entry.getEventIdentifier(),
                entry.getTimestamp(), entry.getPayload().getType().getName(), entry.getPayload().getType().getRevision(), payload, metaData);
    }

}
