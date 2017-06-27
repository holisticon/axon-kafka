package org.axonframework.kafka.example.sender.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class NotificationMarkedEvent {
  String id;
  NotificationState state;
}
