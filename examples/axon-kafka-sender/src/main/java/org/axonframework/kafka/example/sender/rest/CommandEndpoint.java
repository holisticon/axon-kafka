package org.axonframework.kafka.example.sender.rest;

import java.net.URI;
import java.util.UUID;

import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.callbacks.LoggingCallback;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.kafka.example.sender.api.CreateNotificationCommand;
import org.axonframework.kafka.example.sender.api.MarkNotificationCommand;
import org.axonframework.kafka.example.sender.api.NotificationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@RestController
public class CommandEndpoint {

    @Autowired
    CommandGateway gateway;

    @PostMapping(value = "/notification", consumes = "application/json")
    public ResponseEntity<?> createNotification(@RequestBody final Notification notification) {
        final CreateNotificationCommand command = new CreateNotificationCommand(UUID.randomUUID().toString(), notification.getTitle(),
                notification.getMessage());

        gateway.send(GenericCommandMessage.<Object> asCommandMessage(command), LoggingCallback.INSTANCE);

        final URI location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(command.getId()).toUri();

        return ResponseEntity.created(location).build();
    }

    @PatchMapping(value = "/notification/{id}")
    public ResponseEntity<?> markNotification(@PathVariable("id") final String id, @RequestBody boolean markAsRead) {
        final MarkNotificationCommand command = new MarkNotificationCommand(id, markAsRead ? NotificationState.READ : NotificationState.UNREAD);
        gateway.send(GenericCommandMessage.<Object> asCommandMessage(command), LoggingCallback.INSTANCE);
        return ResponseEntity.noContent().build();
    }

}
