#!/usr/bin/env bash

docker-compose -f docker-compose.kafka.yml down
docker network prune