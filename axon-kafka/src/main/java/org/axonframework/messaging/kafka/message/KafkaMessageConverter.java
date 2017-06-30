package org.axonframework.messaging.kafka.message;

import static org.axonframework.serialization.MessageSerializer.serializePayload;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.kafka.message.KafkaMessage.KafkaPayload;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageConverter {

    private final static Logger log = LoggerFactory.getLogger(KafkaMessageConverter.class);
    private Serializer serializer;

    public KafkaMessageConverter(Serializer serializer) {
        this.serializer = serializer;
    }

    public KafkaMessage createKafkaMessage(final EventMessage<?> eventMessage) {
        return createKafkaMessage(serializer, eventMessage);
    }

    public Optional<EventMessage<?>> createAxonEventMessage(final byte[] payload) {
        return createAxonEventMessage(serializer, payload);
    }

    @SuppressWarnings("rawtypes")
    public static KafkaMessage createKafkaMessage(final Serializer serializer, final EventMessage<?> eventMessage) {

        final SerializedObject<byte[]> serializedObject = serializePayload(eventMessage, serializer, byte[].class);
        final Map<String, Object> headers = new HashMap<>();
        eventMessage.getMetaData().forEach((k, v) -> headers.put("axon-metadata-" + k, v));
        headers.put("axon-message-id", eventMessage.getIdentifier());
        headers.put("axon-message-type", serializedObject.getType().getName());
        headers.put("axon-message-revision", serializedObject.getType().getRevision());
        headers.put("axon-message-timestamp", eventMessage.getTimestamp().toString());
        if (eventMessage instanceof DomainEventMessage) {
            headers.put("axon-message-aggregate-id", ((DomainEventMessage) eventMessage).getAggregateIdentifier());
            headers.put("axon-message-aggregate-seq", ((DomainEventMessage) eventMessage).getSequenceNumber());
            headers.put("axon-message-aggregate-type", ((DomainEventMessage) eventMessage).getType());
        }

        final KafkaPayload payload = new KafkaPayload(headers, serializedObject.getData());
        final SerializedObject<byte[]> serializedKafkaPayload = serializer.serialize(payload, byte[].class);

        return new KafkaMessage(eventMessage.getTimestamp().toString(), serializedKafkaPayload.getData());
    }

    static EventMessage<?> toAxonEventMessage(final Serializer serializer, final byte[] payload) {
        try {
            final SimpleSerializedObject<byte[]> serializedKafkaMessage = new SimpleSerializedObject<>(payload, byte[].class, KafkaMessage.class.getName(),
                    null);
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

            final SimpleSerializedObject<byte[]> serializedMessage = new SimpleSerializedObject<>(kafkaPayload.getPayload(), byte[].class,
                    Objects.toString(headers.get("axon-message-type")), Objects.toString(headers.get("axon-message-revision"), null));

            final SerializedMessage<?> message = new SerializedMessage<>(Objects.toString(headers.get("axon-message-id")),
                    new LazyDeserializingObject<>(serializedMessage, serializer), new LazyDeserializingObject<>(MetaData.from(metaData)));

            final String timestamp = Objects.toString(headers.get("axon-message-timestamp"));
            final Instant instant = Instant.parse(timestamp);

            EventMessage<?> responseMessage = null;
            if (headers.containsKey("axon-message-aggregate-id")) {
                responseMessage = new GenericDomainEventMessage<>(Objects.toString(headers.get("axon-message-aggregate-type")),
                        Objects.toString(headers.get("axon-message-aggregate-id")), (Long) headers.get("axon-message-aggregate-seq"), message, () -> instant);
            } else {
                responseMessage = new GenericEventMessage<>(message, () -> instant);
            }
            return responseMessage;

        } catch (Exception e) {
            log.error("Error creating axon message", e);
        }
        return null;
    }

    public static Optional<EventMessage<?>> createAxonEventMessage(final Serializer serializer, final byte[] payload) {
        EventMessage<?> axonEventMessage = toAxonEventMessage(serializer, payload);
        return axonEventMessage != null ? Optional.of(axonEventMessage) : Optional.empty();
    }

}
