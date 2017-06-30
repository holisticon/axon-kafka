#!/usr/bin/env bash

export KAFKA=`docker-machine ip \`docker-machine active\``:9092
export ZOOKEEPER=`docker-machine ip \`docker-machine active\``:2181
export JAVA_HOME="/c/ProgramData/Oracle/Java/javapath"

#kafka-console-producer.sh --broker-list $KAFKA --topic events
#kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic events

kafka-topics --create --zookeeper localhost:2181 --topic events --partitions 1 --replication-factor 1
kafka-topics --create --zookeeper localhost:2181 --topic domain-data --partitions 1 --replication-factor 1

