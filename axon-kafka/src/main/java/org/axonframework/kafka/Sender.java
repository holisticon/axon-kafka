package org.axonframework.kafka;

import org.axonframework.kafka.message.KafkaMessage;

public interface Sender {

    void send(final KafkaMessage message);
}
