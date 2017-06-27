package org.axonframework.kafka.example.sender.model;

import static org.axonframework.commandhandling.model.AggregateLifecycle.apply;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.model.AggregateIdentifier;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.kafka.example.sender.api.CreateNotificationCommand;
import org.axonframework.kafka.example.sender.api.MarkNotificationCommand;
import org.axonframework.kafka.example.sender.api.NotificationCreatedEvent;
import org.axonframework.kafka.example.sender.api.NotificationMarkedEvent;
import org.axonframework.kafka.example.sender.api.NotificationState;
import org.axonframework.spring.stereotype.Aggregate;

@Aggregate
public class Notification {

  @AggregateIdentifier
  String id;
  NotificationState state;

  Notification() {
    // left empty for axon
  }

  @CommandHandler
  public Notification(final CreateNotificationCommand command) {
    apply(new NotificationCreatedEvent(command.getId(), command.getTitle(), command.getMessage()));
  }

  @CommandHandler
  public void mark(MarkNotificationCommand command) {
    if (!this.state.equals(command.getState())) {
      apply(new NotificationMarkedEvent(command.getId(), command.getState()));
    }
  }

  @EventSourcingHandler
  public void on(NotificationCreatedEvent event) {
    this.id = event.getId();
    this.state = NotificationState.UNREAD;
  }

  @EventSourcingHandler
  public void on(NotificationMarkedEvent event) {
    this.state = event.getState();
  }

}
