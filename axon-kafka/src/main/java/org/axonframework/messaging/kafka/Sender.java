package org.axonframework.messaging.kafka;

import org.axonframework.messaging.kafka.message.KafkaMessage;

public interface Sender {

    void send(final KafkaMessage message);
}
