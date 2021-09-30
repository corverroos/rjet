#!/usr/bin/env bash

docker stop nats-main || true
docker run -d --rm --name nats-main -p 4222:4222 -p 6222:6222 -p 8222:8222 nats -js