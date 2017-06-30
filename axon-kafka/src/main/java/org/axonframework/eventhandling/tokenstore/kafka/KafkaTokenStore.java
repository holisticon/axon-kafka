package org.axonframework.eventhandling.tokenstore.kafka;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.Assert;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.GapAwareTrackingToken;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Token store using {@link GlobalSequenceTrackingToken} and a connected {@link KafkaConsumer}.
 * 
 * @author Simon Zambrovski, Holisticon AG
 */
public class KafkaTokenStore implements TokenStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTokenStore.class);
    private final KafkaConsumer<?, ?> consumer;
    private String topic;

    /**
     * Creates a Kafka based token store, which maps the token to position in Kafka.
     * 
     * @param consumer
     *            initialized Kafka Consumer.
     * @param topic
     *            Kafka Topic to calculate offset on.
     */
    public KafkaTokenStore(final KafkaConsumer<?, ?> consumer, final String topic) {
        this.topic = topic;
        this.consumer = consumer;
    }

    @Override
    public TrackingToken fetchToken(String processorName, int segment) throws UnableToClaimTokenException {
        LOGGER.info("Fetch token {}, {}", processorName, segment);
        Optional<Long> offset = Optional.empty();
        try {
            offset = consumer
                    .endOffsets(
                            consumer.partitionsFor(topic).stream().map(info -> new TopicPartition(info.topic(), info.partition())).collect(Collectors.toList()))
                    .values().stream().collect(Collectors.maxBy(Comparator.naturalOrder()));
        } catch (Exception e) {
            LOGGER.error("Error claiming token", e);
            throw new UnableToClaimTokenException("Error claiming a token for processor " + processorName);
        }
        return new GlobalSequenceTrackingToken(offset.orElse(Long.valueOf(0)).longValue());
    }

    @Override
    public void storeToken(final TrackingToken token, String processorName, int segment) throws UnableToClaimTokenException {
        LOGGER.info("Store token {} {}, {}", token, processorName, segment);
        Assert.isTrue(token == null || token instanceof GlobalSequenceTrackingToken,
                () -> String.format("Token [%s] is of the wrong type. Expected [%s]", token, GlobalSequenceTrackingToken.class.getSimpleName()));

        final long offset = ((GlobalSequenceTrackingToken) token).getGlobalIndex();
        try {
            consumer.partitionsFor(topic).stream().map(info -> new TopicPartition(info.topic(), info.partition()))
                    .forEach(partition -> consumer.seek(partition, offset));
        } catch (Exception e) {
            LOGGER.error("Error claiming token", e);
            throw new UnableToClaimTokenException("Error claiming a token for processor " + processorName);
        }
    }

    @Override
    public void releaseClaim(String processorName, int segment) {
        LOGGER.info("Release claim {}, {}", processorName, segment);
    }

}
