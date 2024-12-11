
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

native-test:
	rm -Rf target edd.core
	clojure -J-Dclojure.tools.logging.factory=lambda.logging/slf4j-json-factory -M:jar-native-test
	# This produces pretty bloated config - remove manually all of the _init and $eval classes
	# @java -Dclojure.tools.logging.factory=lambda.logging/slf4j-json-factory -agentlib:native-image-agent=config-merge-dir=resources/META-INF/native-image/com/rbinternational.glms.edd-core -jar target/edd-core-1.0.0-SNAPSHOT-standalone.jar || exit 1
	native-image -jar target/edd-core-1.0.0-SNAPSHOT-standalone.jar edd.core \
		--report-unsupported-elements-at-runtime \
	    --no-fallback \
        --enable-https \
		--no-server \
		--features=clj_easy.graal_build_time.InitClojureClasses \
        -J-Xmx24g \
		 -H:+UnlockExperimentalVMOptions \
        -Dcom.zaxxer.hikari.useWeakReferences=false \
        -Dborkdude.dynaload.aot=true \
        -Dclojure.tools.logging.factory=lambda.logging/slf4j-json-factory
	chmod +x edd.core && ./edd.core
