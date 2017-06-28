package org.axonframework.kafka.example.receiver.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.kafka.SubscribableEventSource;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class SubscribabaleSource implements SubscribableEventSource {

  private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArrayList<>();

  @Override
  public Registration subscribe(final Consumer<List<? extends EventMessage<?>>> messageProcessor) {
    log.info("Added consumer {}", messageProcessor);
    eventProcessors.add(messageProcessor);
    return () -> eventProcessors.remove(messageProcessor);
  }
}
