#!/bin/bash

set -exo pipefail
set -e

mkdir -p modules

cd repl
docker compose down --remove-orphans
docker compose up -d
cd ..

host="https://admin:admin@127.0.0.1:9200"
response="null"
count=1
until [[ "$response" = "200" ]] || [[ $count -gt 15 ]]; do
    response=$(curl -k --write-out %{http_code} --output /dev/null "$host" || echo " Fail (I guess not yet up)")
    >&2 echo "Elastic Search is unavailable ($count) - sleeping:  ${response}"
    sleep 10
    ((count++))
done


echo "Continue"
