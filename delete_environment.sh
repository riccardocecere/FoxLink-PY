#!/usr/bin/env bash

docker-compose -f docker-compose.environment.yml stop
docker-compose -f docker-compose.environment.yml down
docker network prune --force