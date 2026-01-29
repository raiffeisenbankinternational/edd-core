
all: lint test

.PHONY: test
test:
	clj -M:test:it
	clj -M:test:unit

ZPRINT_OPTS := {:binding {:flow? true :indent 0 :nl-separator-all? true}}

lint:
	clj -M:lint

format:
	find src test -name '*.clj' -exec clj -M:format -m zprint.main '$(ZPRINT_OPTS)' -sw {} +
	clj -M:format -m cljfmt.main fix src test

repl:
	clj -M:local:dev:test:nrepl
