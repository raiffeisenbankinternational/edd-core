
MIGRATIONS = ./test/resources/migrations
PG_VIEW_STORE = modules/edd-core-view-store-postgres
CLASSPATH = $(shell clojure -Spath):classes:resources
APP_VERSION = "1.0.0"
APP_GROUP_ID = "app-group-id"
APP_ARTIFACT_ID = "edd-core"
MODULES = $(shell cd modules && ls -1 | sort)
all: lint-fix test-unit test-view-store-postgres
test-unit:
	clojure -M:test:unit
test-it:
	clojure -M:test:it
# Full integration test setup (same as CI pipeline)
it:
	@echo "Running pre-build setup (CloudFormation + Docker + migrations)..."
	./pre-build.sh
	@echo "Running integration tests..."
	bash -c "source ./setup-it-env.sh && clojure -M:test:it"
# Cleanup integration test environment
it-clean:
	@echo "Cleaning up integration test environment..."
	docker compose down --volumes --remove-orphans
	@echo "Integration test environment cleaned"
#
# Run all the known docker-related tests
test-view-store-postgres:
	cd ${PG_VIEW_STORE} && make test
test: test-unit test-it
.PHONY: test test-unit test-it it it-clean
env ?= $(error Please specify the env=... argument, e.g. env=DEV99)
ENV = $(shell echo $(env) | tr a-z A-Z)
rebase:
	git fetch
	git rebase origin/master
drop-cache:
	find . -d -name '.cpcache' -exec rm -rf {} \;
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
# run Postgres only in docker
docker-postgres:
	docker compose up postgres
# run everything in docker
docker-up:
	docker compose up
# Rebuild the docker image from scratch. Useful when CMD changes.
docker-postgres-build:
	docker compose build postgres --no-cache
docker-down:
	docker compose down --remove-orphans
docker-rm:
	docker compose rm --force
docker-psql:
	psql --port 5432 --host localhost -U postgres postgres
docker-migrate:
	docker compose run root-migration
	docker compose run service-migration
	docker compose run migration-test-edd-core
	docker compose run migration-test-dimension
	docker compose run migration-test-application
local-install:
    # EDD Core Installation
	clojure -T:build jar+install  \
		:app-group-id ${APP_GROUP_ID} \
        :app-artifact-id ${APP_ARTIFACT_ID} \
        :app-version '${APP_VERSION}'
    # Modules
	cd modules && \
	for i in ${MODULES}; do \
		cd $$i && \
		clojure -J-Dedd-core.override=${APP_VERSION} -T:build jar+install  \
			:app-group-id ${APP_GROUP_ID} \
			:app-artifact-id $$i \
			:app-version '${APP_VERSION}' && \
		cd ..; \
	done
test-native:
	@rm -Rf target edd.core classes
	@mkdir -p classes
	@clojure -J-Dclojure.tools.logging.factory=lambda.logging/slf4j-json-factory -e "(compile 'edd.core)"
	@native-image -cp ${CLASSPATH} edd.core \
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
changes:
	./changes.py
	./changes.py --shallow --check
