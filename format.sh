#!/bin/bash

set -eo pipefail

if [[ "$1" == "check" ]]; then
  echo "Checking"
  clojure -Sdeps '{:deps {cljfmt/cljfmt {:mvn/version "0.7.0"}}}' -M -m cljfmt.main check src test modules
  if [[ $? -ne 0 ]]; then
    echo "Failed"
    exit 1
  fi
else
  echo "Fixing"
  clojure -Sdeps '{:deps {cljfmt/cljfmt {:mvn/version "0.7.0"}}}' -M -m cljfmt.main fix src test modules
fi
