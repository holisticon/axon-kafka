package org.axonframework.kafka.example.receiver.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;
import org.springframework.stereotype.Service;

import lombok.Getter;

@Service
@Getter
public class SubscribabaleSource implements SubscribableMessageSource<EventMessage<?>> {

  private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArrayList<>();

  @Override
  public Registration subscribe(final Consumer<List<? extends EventMessage<?>>> messageProcessor) {
    eventProcessors.add(messageProcessor);
    return () -> eventProcessors.remove(messageProcessor);
  }
}
