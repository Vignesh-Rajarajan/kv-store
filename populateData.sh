#!/bin/bash

  for shard in localhost:8080 localhost:8081 localhost:8082; do
    echo "Populating data on $shard"
    for i in {1..1000}; do
      curl "http://$shard/set?key=key-$i&value=value-$i" > /dev/null 2>&1
      done
  done
