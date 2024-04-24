#!/bin/bash

set -exo pipefail
exit

#
# Install the old version of docker-compose written in Python.
# Don't bump the versions in the requirements.txt file.
#
pip3 install \
     --trusted-host files.pythonhosted.org \
     --trusted-host pypi.org \
     --trusted-host pypi.python.org \
     --use-feature=2020-resolver \
     -r requirements.txt

export PATH="$PATH:/home/jenkins/.local/bin"

export DOCKER_URL=artifacts.rbi.tech
export DOCKER_ORG=glms-docker-host

#
# Try if docker-compose works
#
docker-compose -v

#
# Drop files produced by Docker (they belong to root)
#
docker run --rm -v `pwd`:/project -w /project alpine:3.19.1 rm -rf .docker

#
# Drop all the nested .cpcache dirs as they lead to
# hours of debugging down the drain.
#
find . -type d -name '.cpcache' -exec rm -rf {} ';'

#
# Stop all the containers
#
docker-compose \
    -f docker-compose.yaml \
    -f docker-compose.clojure.yaml \
    rm --force --stop -v

#
# Run integration tests with Postgres in Docker
#
docker-compose \
    -f docker-compose.yaml \
    -f docker-compose.clojure.yaml \
    run --rm clojure \
    make test-docker
