
MIGRATIONS = ./test/resources/migrations
PG_VIEW_STORE = modules/edd-core-view-store-postgres

all: lint-fix test-unit test-view-store-postgres test-docker

test-unit:
	clojure -M:test:unit

test-integration:
	clojure -M:test:it

#
# Run all the known docker-related tests
#
test-docker:
	cd ${PG_VIEW_STORE} && make test-docker

test-view-store-postgres:
	cd ${PG_VIEW_STORE} && make test-unit

test: test-unit test-integration

.PHONY: test

env ?= $(error Please specify the env=... argument, e.g. env=DEV99)
ENV = $(shell echo $(env) | tr a-z A-Z)

push:
	git push origin HEAD:refs/for/master%topic=env/${ENV}

.PHONY: repl
repl:
	clj -M:local:dev:test:nrepl

lint-check:
	./format.sh check

lint-fix:
	./format.sh

docker-clean:
	rm -rf .docker

docker-up:
	docker compose up

docker-down:
	docker compose down --remove-orphans

docker-rm:
	docker compose rm --force

docker-psql:
	psql --port 55432 --host localhost -U test test
