package org.axonframework.kafka.example.sender.rest;

import lombok.Data;

@Data
public class Notification {
    private String title;
    private String message;
}
