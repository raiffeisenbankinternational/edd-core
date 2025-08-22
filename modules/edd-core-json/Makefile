
all: lint test deps-tree

.PHONY: test
test:
	clj -M:test:unit

lint:
	clj -M:lint
	clj -M:format

repl:
	rm -rf .cpcache
	clj -M:local:dev:test:nrepl

deps-tree:
	clojure -Stree
