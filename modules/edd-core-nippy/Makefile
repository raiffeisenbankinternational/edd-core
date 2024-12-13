
all: lint test

.PHONY: test
test:
	clj -M:test:unit

lint:
	clj -M:lint
	clj -M:format

repl:
	clj -M:local:dev:test:nrepl
