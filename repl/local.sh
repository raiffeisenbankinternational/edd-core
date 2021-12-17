#!/bin/bash

set -e

docker-compose down
if [[ "$(grep vm.max_map_count /etc/sysctl.conf)" == "" ]]; then
  sudo sysctl -w vm.max_map_count=262144
fi
docker-compose up -d

sleep 15
flyway -password="no-secret" \
       -schemas=test \
       -url=jdbc:postgresql://127.0.0.1:5432/postgres?user=postgres \
       -locations="filesystem:${PWD}/../sql/files/edd" migrate