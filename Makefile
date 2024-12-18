
MIGRATIONS = ./test/resources/migrations
PG_VIEW_STORE = modules/edd-core-view-store-postgres

all: lint-fix test-unit test-view-store-postgres test-docker

test-unit:
	clojure -M:test:unit

test-it:
	clojure -M:test:it

#
# Run all the known docker-related tests
#

test-view-store-postgres:
	cd ${PG_VIEW_STORE} && make test

test: test-unit test-it

.PHONY: test

env ?= $(error Please specify the env=... argument, e.g. env=DEV99)
ENV = $(shell echo $(env) | tr a-z A-Z)

rebase:
	git fetch
	git rebase origin/master

push:
	git push origin HEAD:refs/for/master%topic=env/${ENV}

.PHONY: repl
repl:
	PrivateHostedZoneName=lime-${env}.internal.rbigroup.cloud \
	DatabaseEndpoint=localhost \
	clojure -M:cider:local:dev:test:portal

lint-check:
	./format.sh check

lint-fix:
	./format.sh

clean:
	- rm -rf ./.cpcache
	- rm -rf ./.clj-kondo
	- rm -rf ./.lsp

docker-clean:
	rm -rf .docker

docker-pg:
	docker compose up postgres

docker-up:
	docker compose up

# Rebuild the docker image from scratch. Useful when CMD changes.
docker-build-pg:
	docker compose build postgres --no-cache

docker-down:
	docker compose down --remove-orphans

docker-rm:
	docker compose rm --force

docker-psql:
	psql --port 5432 --host localhost -U test test
