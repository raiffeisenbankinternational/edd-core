#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

clojure -M:profiler -e "(require 'bench) (bench/run-all)"
