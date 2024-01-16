test-unit:
	clj -M:test:unit

test-integration:
	clj -M:test:it

test: test-unit test-integration

.PHONY: test

env ?= $(error Please specify the env=... argument, e.g. env=DEV99)
ENV = $(shell echo $(env) | tr a-z A-Z)

push:
	git push origin HEAD:refs/for/master%topic=env/${ENV}
