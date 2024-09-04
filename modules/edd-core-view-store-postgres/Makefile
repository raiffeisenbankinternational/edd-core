env ?= $(error Please specify the env=... argument, e.g. env=DEV99)
ENV = $(shell echo $(env) | tr a-z A-Z)

all: test

.PHONY: repl
repl:
	PrivateHostedZoneName=lime-${env}.internal.rbigroup.cloud \
	DatabaseEndpoint=localhost \
	clojure -M:local:dev:test:nrepl:cider:portal

test-unit:
	clojure -M:test:unit

test-docker:
	clojure -M:test:docker

test: test-unit test-docker

.PHONY: test

slug ?= $(error Please specify the slug=... argument)
migration_id = $(shell date +'%Y%m%d%H%M%S')

MIGRATIONS = ./test/resources/migrations

create-migration:
	touch ${MIGRATIONS}/${migration_id}-${slug}.up.sql

agg_file ?= $(error Please specify the agg_file=... argument)

generate-sql:
	cat ${agg_file} | sed \
		-e "s/\"/'/g" -e 's/: /, /g' \
		-e 's/{/jsonb_build_object(/g' \
		-e 's/}/\)/g' \
		-e 's/\[/jsonb_build_array(/g' \
		-e 's/\]/)/g'
