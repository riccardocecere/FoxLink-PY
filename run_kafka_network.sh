#!/usr/bin/env bash

echo Starting the network...

docker network create kafka-network
docker-compose -f docker-compose.kafka.yml build
docker-compose -f docker-compose.kafka.yml up