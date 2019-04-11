#!/usr/bin/env bash

echo Starting the network...

docker network create environment-network
docker-compose -f docker-compose.environment.yml build
docker-compose -f docker-compose.environment.yml up