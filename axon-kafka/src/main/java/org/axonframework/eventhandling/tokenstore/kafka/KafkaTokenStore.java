package org.axonframework.eventhandling.tokenstore.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.Assert;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
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
    private TopicPartition topicPartition;

    /**
     * Creates a Kafka based token store, which maps the token to position in Kafka.
     * 
     * @param consumer
     *            initialized Kafka Consumer.
     * @param topic
     *            Kafka Topic to calculate offset on.
     */
    public KafkaTokenStore(final KafkaConsumer<?, ?> consumer, final String topic) {
        this.consumer = consumer;
        final List<PartitionInfo> consumerPartitions = this.consumer.partitionsFor(topic);
        Assert.isTrue(consumerPartitions.size() != 0, () -> String.format("The topic %s has %d partitions, but exactly 1 is expected.", topic));
        this.topicPartition = new TopicPartition(topic, consumerPartitions.get(0).partition());
    }

    @Override
    public TrackingToken fetchToken(String processorName, int segment) throws UnableToClaimTokenException {
        LOGGER.info("Fetch token {}, {}", processorName, segment);
        Optional<Long> offset = Optional.empty();
        try {
            offset = Optional.of(consumer.endOffsets(Arrays.asList(this.topicPartition)).get(this.topicPartition));
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
            consumer.seek(topicPartition, offset);
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
