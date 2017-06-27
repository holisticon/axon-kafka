#!/bin/bash

export DOCKER_IP=`docker-machine ip \`docker-machine active\``

docker run -p 2181:2181 -p 9092:9092 -d --env ADVERTISED_HOST=$DOCKER_IP --env ADVERTISED_PORT=9092 spotify/kafka

