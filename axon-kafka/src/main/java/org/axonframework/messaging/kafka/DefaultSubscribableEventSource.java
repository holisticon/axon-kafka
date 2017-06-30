package org.axonframework.messaging.kafka;

import java.util.List;
import java.util.function.Consumer;

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;

public interface DefaultSubscribableEventSource extends SubscribableMessageSource<EventMessage<?>> {

  List<Consumer<List<? extends EventMessage<?>>>> getEventProcessors();

  @Override
  default Registration subscribe(final Consumer<List<? extends EventMessage<?>>> messageProcessor) {
    getEventProcessors().add(messageProcessor);
    return () -> getEventProcessors().remove(messageProcessor);
  }
}
