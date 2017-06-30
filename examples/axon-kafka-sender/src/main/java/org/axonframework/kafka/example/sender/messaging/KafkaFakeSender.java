package org.axonframework.kafka.example.sender.messaging;

import org.axonframework.messaging.kafka.Sender;
import org.axonframework.messaging.kafka.message.KafkaMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaFakeSender implements Sender {

    private String eventTopic;

    public KafkaFakeSender(final String eventTopic) {
	this.eventTopic = eventTopic;
    }

    @Override
    public void send(KafkaMessage message) {
	log.info("[FAKE SENDER] -> Sending message '{}' with size {} to '{}'", message.getKey(),
		message.getPayload().length, eventTopic);
    }

}
