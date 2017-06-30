package org.axonframework.kafka.example.sender.event;

import org.axonframework.eventhandling.EventHandler;
import org.axonframework.kafka.example.sender.api.NotificationCreatedEvent;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class NotificationEventHandler {

    @EventHandler
    public void on(NotificationCreatedEvent e) {
        log.info("Local tracking handler {}", e);
    }
}
