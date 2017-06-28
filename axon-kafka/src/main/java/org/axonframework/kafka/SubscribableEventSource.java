package org.axonframework.kafka;

import java.util.List;
import java.util.function.Consumer;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;

public interface SubscribableEventSource extends SubscribableMessageSource<EventMessage<?>> {

  List<Consumer<List<? extends EventMessage<?>>>> getEventProcessors();

}
