package org.axonframework.messaging.kafka.message;

import java.util.Map;

public class KafkaMessage {

  private String key;
  private byte[] payload;

  public KafkaMessage() {
  }

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

    private Map<String, Object> headers;
    private byte[] payload;

    public KafkaPayload() {
    }

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