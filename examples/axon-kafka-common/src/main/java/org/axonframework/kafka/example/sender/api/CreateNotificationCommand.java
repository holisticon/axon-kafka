package org.axonframework.kafka.example.sender.api;

import org.axonframework.commandhandling.TargetAggregateIdentifier;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateNotificationCommand {
    @TargetAggregateIdentifier
    String id;
    String title;
    String message;
}
