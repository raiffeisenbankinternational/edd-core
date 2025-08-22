#!/bin/bash

set -exo pipefail
set -e

### Let's deal with the changes before stepping further.

# Run changes.sh to update CHANGES.md
./changes.sh

# Check if CHANGES.md has changes
if [[ -n $(git diff --name-only CHANGES.md) ]]; then
    echo "CHANGES.md has been updated. Please review the changes and commit them."
    exit 1
fi

### end changes

docker container prune --force

mkdir -p modules

docker compose down --remove-orphans
docker compose up -d

host="https://admin:admin@127.0.0.1:9200"
response="null"
count=1
until [[ "$response" = "200" ]] || [[ $count -gt 15 ]]; do
    response=$(curl -k --write-out %{http_code} --output /dev/null "$host" || echo " Fail (I guess not yet up)")
    >&2 echo "Elastic Search is unavailable ($count) - sleeping:  ${response}"
    sleep 10
    ((count++))
done

docker ps
docker compose logs postgres

echo "Running Postgres migrations"

docker compose run root-migration
docker compose run service-migration
docker compose run migration-test-edd-core
docker compose run migration-test-dimension

echo "Continue"
