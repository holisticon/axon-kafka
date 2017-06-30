package org.axonframework.config.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaConfigBuilderTest {

    @Test
    public void testConsumer() {
        Properties props = KafkaConfigBuilder.defaultConsumer().bootstrapServers("server").group("group").withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class).build();
        
        assertEquals("group", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("server", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringDeserializer.class.getName(), props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArrayDeserializer.class.getName(), props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        
    }

    @Test
    public void testConsumerAsMap() {
        Map<String, Object> props = KafkaConfigBuilder.defaultConsumer().bootstrapServers("server").group("group").withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class).asMap();
        assertEquals("group", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("server", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringDeserializer.class.getName(), props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArrayDeserializer.class.getName(), props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

}
