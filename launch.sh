#!/bin/bash

set -e

trap 'killall distribut 2>/dev/null' SIGINT

cd "$(dirname "$0")"

killall distribut 2>/dev/null || true

lsof -n -i:8080 | grep LISTEN | awk '{ print $2 }' | uniq | xargs kill -9
lsof -n -i:8081 | grep LISTEN | awk '{ print $2 }' | uniq | xargs kill -9
lsof -n -i:8082 | grep LISTEN | awk '{ print $2 }' | uniq | xargs kill -9

sleep 1

go install -v

sleep 1

distributed-kv-store -db-location=luffy.db -http-addr=127.0.0.1:8080 -config-file=sharding.toml -shard=luffy &
#distributed-kv-store -db-location=luffy.db -http-addr=127.0.0.22:8080 -config-file=sharding.toml -shard=luffy -replica=true &

distributed-kv-store -db-location=zoro.db -http-addr=127.0.0.1:8081 -config-file=sharding.toml -shard=zoro &
#distributed-kv-store -db-location=zoro.db -http-addr=127.0.0.33:8081 -config-file=sharding.toml -shard=zoro -replica &

distributed-kv-store -db-location=nami.db -http-addr=127.0.0.1:8082 -config-file=sharding.toml -shard=nami &
#distributed-kv-store -db-location=nami.db -http-addr=127.0.0.44:8082 -config-file=sharding.toml -shard=nami -replica &

wait

