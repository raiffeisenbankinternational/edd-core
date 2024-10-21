#!/bin/bash

set -e

docker compose down
if [[ "$(grep vm.max_map_count /etc/sysctl.conf)" == "" ]]; then
  sudo sysctl -w vm.max_map_count=262144
fi

docker compose up postgres -d
sleep 5

docker compose run root-migration
docker compose run service-migration
