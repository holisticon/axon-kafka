# Axon Kafka Eventstore

## Goal

The goal of this project is to implement a Kafka-based EventStore.

## Structure

The project consists of the axon-kafka library and some examples. Examples consists of a sender and a receiver. The sender is providing the REST API (use Swagger calling http://localhost:8080/swagger-ui.html) and create some notifications. Every creation will response with a notification id which can be used in order to change the state of the notification.


## Conventions / Ideas

- The axon-kafka library should only depend on Kafka and AxonFramework.
- Examples are implemented using SpringBoot.
- There will be axon-kafka-springboot component for convenient usage of Axon Kafka (later).

## Progress / Features
- Kafka sender is implemented and sends the messages 
