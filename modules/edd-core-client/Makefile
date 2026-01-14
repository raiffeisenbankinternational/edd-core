
all: lint test

.PHONY: test
test:
	clj -M:test:it
	clj -M:test:unit

lint:
	clj-kondo --lint src --debug
	cljfmt check src

fix:
	cljfmt fix src

repl:
	clj -M:local:dev:test:nrepl
