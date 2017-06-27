package org.axonframework.kafka.example.sender.messaging;

import org.axonframework.kafka.Sender;
import org.axonframework.kafka.message.KafkaMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaSender implements Sender {

    private final String eventTopic;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaSender(final String eventTopic, KafkaTemplate<String, byte[]> kafkaTemplate) {
	this.eventTopic = eventTopic;
	this.kafkaTemplate = kafkaTemplate;
    }

    public void send(final KafkaMessage message) {
	// the KafkaTemplate provides asynchronous send methods returning a
	// Future
	final ListenableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(eventTopic, message.getKey(),
		message.getPayload());

	// register a callback with the listener to receive the result of the
	// send
	// asynchronously
	future.addCallback(new ListenableFutureCallback<SendResult<String, byte[]>>() {

	    @Override
	    public void onSuccess(SendResult<String, byte[]> result) {
		log.info("sent message='{}' with offset={}", message, result.getRecordMetadata().offset());
	    }

	    @Override
	    public void onFailure(Throwable ex) {
		log.error("unable to send message='{}'", message, ex);
	    }
	});

	// or, to block the sending thread to await the result, invoke the
	// future's
	// get() method
    }
}
