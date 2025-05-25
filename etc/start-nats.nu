#!/bin/env nu
docker run --rm -p 4222:4222 -v $"(pwd)/nats-config.conf:/nats-server.conf" nats
