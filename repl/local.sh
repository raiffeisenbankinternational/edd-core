#/bin/bash

set -e

docker rm -f "postgres" || echo "Missing"
docker run -d --name postgres \
            -p 5432:5432 \
            -e POSTGRES_PASSWORD=no-secret \
            -d postgres
sleep 10
flyway -password="no-secret" \
       -schemas=glms \
       -url=jdbc:postgresql://127.0.0.1:5432/postgres?user=postgres \
       -locations="filesystem:${PWD}/../sql/files" migrate
