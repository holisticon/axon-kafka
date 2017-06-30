package org.axonframework.eventsourcing.eventstore.kafka;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GenericDomainEventEntry;
import org.axonframework.eventsourcing.eventstore.GenericTrackedDomainEventEntry;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.kafka.message.KafkaMessage;
import org.axonframework.messaging.kafka.message.KafkaMessage.KafkaPayload;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStorageConverter {

    private static final Logger log = LoggerFactory.getLogger(KafkaStorageConverter.class);

    GenericTrackedDomainEventEntry<?> toEntry(final ConsumerRecord<String, byte[]> record, final Serializer serializer) {
        try {
            final SimpleSerializedObject<byte[]> serializedKafkaMessage = new SimpleSerializedObject<>(record.value(), byte[].class,
                    KafkaMessage.class.getName(), null);
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

            String payloadRevision = Objects.toString(headers.get("axon-message-revision"), null);
            String payloadType = Objects.toString(headers.get("axon-message-type"));
            final String timestamp = Objects.toString(headers.get("axon-message-timestamp"));
            final Instant instant = Instant.parse(timestamp);

            String aggregateIdentifier = Objects.toString(headers.get("axon-message-aggregate-id"));
            long sequenceNumber = (Long) headers.get("axon-message-aggregate-seq");
            String eventIdentifier = Objects.toString(headers.get("axon-message-id"));

            String type = Objects.toString(headers.get("axon-message-aggregate-type"));
            TrackingToken token = new GlobalSequenceTrackingToken(record.offset());

            byte[] entryMetadata = serializer.serialize(MetaData.from(metaData), byte[].class).getData();

            return new GenericTrackedDomainEventEntry<byte[]>(token, type, aggregateIdentifier, sequenceNumber, eventIdentifier, instant, payloadType,
                    payloadRevision, kafkaPayload.getPayload(), entryMetadata);

        } catch (Exception e) {
            log.error("Error creating axon message", e);
        }

        return null;
    }

    public TrackedEventData<?> createTrackedEventEntry(final ConsumerRecord<String, byte[]> record, final Serializer serializer) {
        return toEntry(record, serializer);
    }

    @SuppressWarnings("unchecked")
    public DomainEventData<?> createDomainEventEntry(final ConsumerRecord<String, byte[]> record, final Serializer serializer) {
        GenericTrackedDomainEventEntry<?> entry = toEntry(record, serializer);
        byte[] payload = ((SimpleSerializedObject<byte[]>) entry.getPayload()).getData();
        byte[] metaData = ((SerializedMetaData<byte[]>) entry.getMetaData()).getData();
        return new GenericDomainEventEntry<byte[]>(entry.getType(), entry.getAggregateIdentifier(), entry.getSequenceNumber(), entry.getEventIdentifier(),
                entry.getTimestamp(), entry.getPayload().getType().getName(), entry.getPayload().getType().getRevision(), payload, metaData);
    }

}
