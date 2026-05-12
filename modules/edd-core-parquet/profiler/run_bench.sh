#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

split_csv() {
  local input="$1"
  local item
  local old_ifs="$IFS"

  IFS=','
  read -r -a items <<< "$input"
  IFS="$old_ifs"

  for item in "${items[@]}"; do
    item="$(echo "$item" | xargs)"
    if [[ -n "$item" ]]; then
      printf '%s\n' "$item"
    fi
  done
}

run_clojure_bench() {
  clojure -M:profiler -m bench "$@"
}

run_compare_isolated() {
  local scenarios_csv="${PARQUET_BENCH_SCHEMAS:-mixed,string-heavy,numeric-heavy,enum-heavy}"
  local row_counts_csv="${PARQUET_BENCH_ROW_COUNTS:-100,1000,5000,10000,50001,2000000}"

  printf '\n====================================\n'
  printf '  Parquet Writer Compare Isolated\n'
  printf '====================================\n'
  printf 'Date: %s\n' "$(date --iso-8601=seconds)"
  printf 'JVM: %s\n' "$(java -version 2>&1 | head -n 1 | sed 's/.*version "\([^"]*\)".*/\1/')"
  printf 'Threads: %s\n' "${PARQUET_BENCH_THREADS:-default}"
  printf 'Row counts: [%s]\n' "$row_counts_csv"
  printf 'Scenarios: [%s]\n' "$scenarios_csv"

  while IFS= read -r scenario; do
    printf '\n=== %s ===\n' "$scenario"
    while IFS= read -r row_count; do
      run_clojure_bench one "$scenario" "$row_count" counted
      run_clojure_bench one "$scenario" "$row_count" lazy-default
      run_clojure_bench one "$scenario" "$row_count" lazy-single
    done < <(split_csv "$row_counts_csv")
  done < <(split_csv "$scenarios_csv")

  printf '\n====================================\n'
  printf '  Compare Isolated Complete\n'
  printf '====================================\n\n'
}

run_tune_isolated() {
  local scenarios_csv="${PARQUET_BENCH_TUNE_SCHEMAS:-mixed}"
  local row_counts_csv="${PARQUET_BENCH_TUNE_ROW_COUNTS:-2000000}"
  local chunk_sizes_csv="${PARQUET_BENCH_TUNE_CHUNK_SIZES:-25000,50000,100000}"
  local in_flight_csv="${PARQUET_BENCH_TUNE_MAX_IN_FLIGHT:-2,4,8}"

  printf '\n====================================\n'
  printf '  Parquet Writer Lazy Tuning Isolated\n'
  printf '====================================\n'
  printf 'Date: %s\n' "$(date --iso-8601=seconds)"
  printf 'JVM: %s\n' "$(java -version 2>&1 | head -n 1 | sed 's/.*version "\([^"]*\)".*/\1/')"
  printf 'Threads: %s\n' "${PARQUET_BENCH_THREADS:-default}"
  printf 'Row counts: [%s]\n' "$row_counts_csv"
  printf 'Scenarios: [%s]\n' "$scenarios_csv"
  printf 'Chunk sizes: [%s]\n' "$chunk_sizes_csv"
  printf 'Max in flight: [%s]\n' "$in_flight_csv"

  while IFS= read -r scenario; do
    printf '\n=== %s ===\n' "$scenario"
    while IFS= read -r row_count; do
      run_clojure_bench one "$scenario" "$row_count" counted
      run_clojure_bench one "$scenario" "$row_count" lazy-default
      while IFS= read -r chunk_size; do
        while IFS= read -r max_in_flight; do
          run_clojure_bench one "$scenario" "$row_count" lazy-custom "$chunk_size" "$max_in_flight"
        done < <(split_csv "$in_flight_csv")
      done < <(split_csv "$chunk_sizes_csv")
    done < <(split_csv "$row_counts_csv")
  done < <(split_csv "$scenarios_csv")

  printf '\n====================================\n'
  printf '  Tuning Isolated Complete\n'
  printf '====================================\n\n'
}

command="${1:-compare}"
shift || true

case "$command" in
  compare|quick|tune|all|one)
    run_clojure_bench "$command" "$@"
    ;;
  compare-isolated)
    run_compare_isolated "$@"
    ;;
  tune-isolated)
    run_tune_isolated "$@"
    ;;
  *)
    echo "Unknown command: $command" >&2
    echo "Usage: ./profiler/run_bench.sh [compare|quick|tune|all|one|compare-isolated|tune-isolated]" >&2
    exit 1
    ;;
esac
