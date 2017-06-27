package org.axonframework.kafka.example.sender.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class NotificationCreatedEvent {
  String id;
  String title;
  String message;
}
