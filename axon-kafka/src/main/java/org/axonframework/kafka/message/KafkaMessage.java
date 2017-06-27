package org.axonframework.kafka.message;

import java.util.Map;

public class KafkaMessage {

    private final String key;
    private final byte[] payload;

    public KafkaMessage(String key, byte[] payload) {
	this.key = key;
	this.payload = payload;
    }

    public String getKey() {
	return key;
    }

    public byte[] getPayload() {
	return payload;
    }

    public static class KafkaPayload {

	private final Map<String, Object> headers;
	private final byte[] payload;

	public KafkaPayload(Map<String, Object> headers, byte[] payload) {
	    this.headers = headers;
	    this.payload = payload;
	}

	public Map<String, Object> getHeaders() {
	    return headers;
	}

	public byte[] getPayload() {
	    return payload;
	}

    }
}