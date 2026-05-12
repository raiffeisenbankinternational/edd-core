(ns bench
  "Benchmarks for parquet writer performance.

   Run with:
   - ./profiler/run_bench.sh
   - ./profiler/run_bench.sh quick
   - ./profiler/run_bench.sh tune
   - ./profiler/run_bench.sh compare-isolated
   - ./profiler/run_bench.sh tune-isolated"
  (:require [clojure.string :as str]
            [edd.parquet.core :as parquet]))

(set! *warn-on-reflection* true)

;;; Test Schemas - varying column types to test different paths

(def schema-mixed
  "Mixed schema with all column types"
  {:description "Benchmark mixed schema"
   :columns [["ID" :uuid "ID" :required]
             ["NAME" :string "Name" :required]
             ["AMOUNT" :double "Amount" :required]
             ["QUANTITY" :long "Quantity" :required]
             ["STATUS" :enum "Status" :required :enum ["PENDING" "ACTIVE" "CLOSED"]]
             ["IS_ACTIVE" :boolean "Active flag" :optional]
             ["CREATED" :date "Date" :optional]]})

(def schema-string-heavy
  "Schema with mostly string columns"
  {:description "String heavy schema"
   :columns [["ID" :uuid "ID" :required]
             ["NAME" :string "Name" :required]
             ["DESCRIPTION" :string "Description" :optional]
             ["CATEGORY" :string "Category" :required]
             ["NOTES" :string "Notes" :optional]]})

(def schema-numeric-heavy
  "Schema with mostly numeric columns"
  {:description "Numeric heavy schema"
   :columns [["ID" :long "ID" :required]
             ["AMOUNT1" :double "Amount 1" :required]
             ["AMOUNT2" :double "Amount 2" :required]
             ["AMOUNT3" :double "Amount 3" :required]
             ["COUNT1" :long "Count 1" :required]
             ["COUNT2" :long "Count 2" :required]]})

(def schema-enum-heavy
  "Schema with multiple enum columns"
  {:description "Enum heavy schema"
   :columns [["ID" :uuid "ID" :required]
             ["STATUS" :enum "Status" :required :enum ["PENDING" "ACTIVE" "CLOSED" "CANCELLED"]]
             ["TYPE" :enum "Type" :required :enum ["A" "B" "C" "D" "E"]]
             ["REGION" :enum "Region" :required :enum ["NORTH" "SOUTH" "EAST" "WEST"]]
             ["PRIORITY" :enum "Priority" :required :enum ["LOW" "MEDIUM" "HIGH" "CRITICAL"]]]})

(def scenarios
  [{:key :mixed
    :name "Mixed Schema (typical usage)"
    :schema schema-mixed}
   {:key :string-heavy
    :name "String Heavy Schema"
    :schema schema-string-heavy}
   {:key :numeric-heavy
    :name "Numeric Heavy Schema"
    :schema schema-numeric-heavy}
   {:key :enum-heavy
    :name "Enum Heavy Schema"
    :schema schema-enum-heavy}])

(def scenarios-by-key
  (into {}
        (map (fn [scenario]
               [(:key scenario) scenario]))
        scenarios))

(def scenario-ids
  (into {}
        (map-indexed (fn [idx scenario]
                       [(:key scenario) (inc idx)]))
        scenarios))

;;; Row Generators

(defn- random-uuid-str
  [^java.util.Random rng]
  (str (java.util.UUID. (.nextLong rng)
                        (.nextLong rng))))

(defn- random-choice
  [^java.util.Random rng values]
  (nth values (.nextInt rng (count values))))

(defn- generate-mixed-row
  [^java.util.Random rng ^long i]
  {"ID" (random-uuid-str rng)
   "NAME" (str "Customer-" i)
   "AMOUNT" (* (.nextDouble rng) 10000.0)
   "QUANTITY" (long (.nextInt rng 1000))
   "STATUS" (random-choice rng ["PENDING" "ACTIVE" "CLOSED"])
   "IS_ACTIVE" (random-choice rng [true false nil])
   "CREATED" "2024-01-15"})

(defn- generate-string-row
  [^java.util.Random rng ^long i]
  {"ID" (random-uuid-str rng)
   "NAME" (str "Customer-" i "-" (random-uuid-str rng))
   "DESCRIPTION" (str "This is longer description for item " i " with some additional text")
   "CATEGORY" (str "Category-" (mod i 100))
   "NOTES" (when (zero? (mod i 3)) (str "Notes for " i))})

(defn- generate-numeric-row
  [^java.util.Random rng ^long i]
  {"ID" i
   "AMOUNT1" (* (.nextDouble rng) 10000.0)
   "AMOUNT2" (* (.nextDouble rng) 5000.0)
   "AMOUNT3" (* (.nextDouble rng) 1000.0)
   "COUNT1" (long (.nextInt rng 10000))
   "COUNT2" (long (.nextInt rng 5000))})

(defn- generate-enum-row
  [^java.util.Random rng ^long _i]
  {"ID" (random-uuid-str rng)
   "STATUS" (random-choice rng ["PENDING" "ACTIVE" "CLOSED" "CANCELLED"])
   "TYPE" (random-choice rng ["A" "B" "C" "D" "E"])
   "REGION" (random-choice rng ["NORTH" "SOUTH" "EAST" "WEST"])
   "PRIORITY" (random-choice rng ["LOW" "MEDIUM" "HIGH" "CRITICAL"])})

(def row-generators
  {:mixed generate-mixed-row
   :string-heavy generate-string-row
   :numeric-heavy generate-numeric-row
   :enum-heavy generate-enum-row})

(defn- dataset-seed
  [base-seed scenario-key row-count]
  (let [scenario-id
        (long (get scenario-ids scenario-key 0))

        row-component
        (unchecked-multiply 1000003 (long row-count))

        scenario-component
        (unchecked-multiply 1009 scenario-id)]
    (unchecked-add (unchecked-add (long base-seed)
                                  row-component)
                   scenario-component)))

(defn generate-rows
  "Generate n rows for scenario using deterministic seed.
   Returns vector so isolated JVM runs use identical data."
  [scenario-key n base-seed]
  (let [generator
        (get row-generators scenario-key)

        seed
        (dataset-seed base-seed scenario-key n)

        rng
        (java.util.Random. seed)]
    (mapv (fn [i]
            (generator rng i))
          (range n))))

(defn- make-lazy-rows
  [rows]
  (map identity rows))

;;; Config helpers

(defn- parse-long-value
  [s]
  (Long/parseLong (str/trim s)))

(defn- split-csv
  [s]
  (let [parts
        (str/split (or s "") #",")]
    (into []
          (comp (map str/trim)
                (remove str/blank?))
          parts)))

(defn- env-long
  [env-key default]
  (if-let [raw
           (System/getenv env-key)]
    (parse-long-value raw)
    default))

(defn- env-csv-longs
  [env-key default]
  (if-let [raw
           (System/getenv env-key)]
    (mapv parse-long-value (split-csv raw))
    default))

(defn- env-csv-keywords
  [env-key default]
  (if-let [raw
           (System/getenv env-key)]
    (mapv keyword (split-csv raw))
    default))

(defn- env-keyword
  [env-key default]
  (if-let [raw
           (System/getenv env-key)]
    (keyword (str/trim raw))
    default))

(defn- common-config
  []
  {:threads (env-long "PARQUET_BENCH_THREADS"
                      (.. Runtime getRuntime availableProcessors))
   :data-seed (env-long "PARQUET_BENCH_DATA_SEED" 424242)
   :order-seed (env-long "PARQUET_BENCH_ORDER_SEED" 20260512)})

(defn- compare-config
  []
  (let [common
        (common-config)]
    (merge common
           {:row-counts (env-csv-longs "PARQUET_BENCH_ROW_COUNTS"
                                       [100 1000 5000 10000 50001 2000000])
            :warmup-iterations (env-long "PARQUET_BENCH_WARMUP" 5)
            :bench-iterations (env-long "PARQUET_BENCH_ITERS" 20)
            :compression (env-keyword "PARQUET_BENCH_COMPRESSION" :snappy)
            :scenario-keys (env-csv-keywords "PARQUET_BENCH_SCHEMAS"
                                             [:mixed :string-heavy :numeric-heavy :enum-heavy])})))

(defn- quick-config
  []
  (let [common
        (common-config)]
    (merge common
           {:row-counts [100 1000]
            :warmup-iterations 2
            :bench-iterations 5
            :compression :snappy
            :scenario-keys [:mixed :enum-heavy]})))

(defn- tune-config
  []
  (let [common
        (common-config)]
    (merge common
           {:row-counts (env-csv-longs "PARQUET_BENCH_TUNE_ROW_COUNTS"
                                       [2000000])
            :warmup-iterations (env-long "PARQUET_BENCH_TUNE_WARMUP" 2)
            :bench-iterations (env-long "PARQUET_BENCH_TUNE_ITERS" 5)
            :compression (env-keyword "PARQUET_BENCH_TUNE_COMPRESSION" :snappy)
            :scenario-keys (env-csv-keywords "PARQUET_BENCH_TUNE_SCHEMAS"
                                             [:mixed])
            :chunk-sizes (env-csv-longs "PARQUET_BENCH_TUNE_CHUNK_SIZES"
                                        [25000 50000 100000])
            :max-in-flight-chunks (env-csv-longs "PARQUET_BENCH_TUNE_MAX_IN_FLIGHT"
                                                 [2 4 8])})))

(defn- one-config
  []
  (let [common
        (common-config)]
    (merge common
           {:warmup-iterations (env-long "PARQUET_BENCH_ONE_WARMUP"
                                         (env-long "PARQUET_BENCH_WARMUP" 5))
            :bench-iterations (env-long "PARQUET_BENCH_ONE_ITERS"
                                        (env-long "PARQUET_BENCH_ITERS" 20))
            :compression (env-keyword "PARQUET_BENCH_ONE_COMPRESSION"
                                      (env-keyword "PARQUET_BENCH_COMPRESSION"
                                                   :snappy))})))

(defn- select-scenarios
  [scenario-keys]
  (filterv (fn [scenario]
             (contains? (set scenario-keys) (:key scenario)))
           scenarios))

(defn- lookup-scenario
  [scenario-key]
  (or (get scenarios-by-key scenario-key)
      (throw (ex-info "Unknown scenario key"
                      {:scenario-key scenario-key
                       :known-scenarios (vec (keys scenarios-by-key))}))))

;;; Mode specs

(defn- resolved-lazy-default-chunk-size
  []
  (long (var-get (ns-resolve 'edd.parquet.core
                             'lazy-parallel-default-chunk-size))))

(defn- counted-mode-spec
  [threads]
  {:kind :counted
   :label "counted"
   :rows-fn identity
   :write-opts {:threads threads}})

(defn- lazy-default-mode-spec
  [threads]
  (let [chunk-size
        (resolved-lazy-default-chunk-size)

        max-in-flight-chunks
        threads]
    {:kind :lazy-default
     :label (format "lazy-default[c=%d,f=%d]"
                    chunk-size
                    max-in-flight-chunks)
     :rows-fn make-lazy-rows
     :write-opts {:threads threads}
     :resolved-chunk-size chunk-size
     :resolved-max-in-flight-chunks max-in-flight-chunks}))

(defn- lazy-single-mode-spec
  [threads]
  {:kind :lazy-single
   :label "lazy-single"
   :rows-fn make-lazy-rows
   :write-opts {:threads threads
                :lazy-parallel? false}})

(defn- lazy-custom-mode-spec
  [threads chunk-size max-in-flight-chunks]
  {:kind :lazy-custom
   :label (format "lazy[c=%d,f=%d]"
                  chunk-size
                  max-in-flight-chunks)
   :rows-fn make-lazy-rows
   :write-opts {:threads threads
                :chunk-size chunk-size
                :max-in-flight-chunks max-in-flight-chunks}})

(defn- compare-mode-specs
  [threads]
  [(counted-mode-spec threads)
   (lazy-default-mode-spec threads)
   (lazy-single-mode-spec threads)])

(defn- tuned-lazy-mode-specs
  [threads chunk-sizes max-in-flight-chunks]
  (let [custom-modes
        (for [chunk-size chunk-sizes
              max-in-flight-chunk max-in-flight-chunks]
          (lazy-custom-mode-spec threads
                                 chunk-size
                                 max-in-flight-chunk))]
    (vec (concat [(counted-mode-spec threads)
                  (lazy-default-mode-spec threads)]
                 custom-modes))))

(defn- mode-spec-from-cli
  [threads mode-kind chunk-size max-in-flight-chunks]
  (case mode-kind
    "counted" (counted-mode-spec threads)
    "lazy-default" (lazy-default-mode-spec threads)
    "lazy-single" (lazy-single-mode-spec threads)
    "lazy-custom"
    (do
      (when-not (and chunk-size max-in-flight-chunks)
        (throw (ex-info "lazy-custom requires chunk-size and max-in-flight-chunks"
                        {:mode-kind mode-kind})))
      (lazy-custom-mode-spec threads
                             (parse-long-value chunk-size)
                             (parse-long-value max-in-flight-chunks)))
    (throw (ex-info "Unknown mode kind"
                    {:mode-kind mode-kind
                     :known-mode-kinds ["counted"
                                        "lazy-default"
                                        "lazy-single"
                                        "lazy-custom"]}))))

;;; Benchmark Utilities

(defn- measure-time-ms
  [f]
  (let [start
        (System/nanoTime)

        result
        (f)

        end
        (System/nanoTime)]
    [result (/ (- end start) 1000000.0)]))

(defn- write-once
  [schema rows {:keys [rows-fn write-opts]} config]
  (parquet/write-parquet-bytes
   (merge {:table-name "bench"
           :schema schema
           :rows (rows-fn rows)
           :compression (:compression config)}
          write-opts)))

(defn- shuffle-mode-specs
  [^java.util.Collection mode-specs order-seed iteration-idx]
  (let [shuffled
        (java.util.ArrayList. mode-specs)

        seed
        (unchecked-add (long order-seed)
                       (long iteration-idx))

        rng
        (java.util.Random. seed)]
    (java.util.Collections/shuffle shuffled rng)
    (vec shuffled)))

(defn- empty-samples
  [mode-specs]
  (into {}
        (map (fn [mode-spec]
               [(:label mode-spec) {:times []
                                    :bytes nil}]))
        mode-specs))

(defn- add-sample
  [samples label time-ms byte-size]
  (let [sample
        (get samples label)]
    (assoc samples
           label
           {:times (conj (:times sample) time-ms)
            :bytes (or (:bytes sample) byte-size)})))

(defn- sample-stats
  [{:keys [times bytes]}]
  (let [sorted-times
        (vec (sort times))

        n
        (count sorted-times)]
    {:min (first sorted-times)
     :max (last sorted-times)
     :mean (/ (reduce + times) n)
     :median (nth sorted-times (quot n 2))
     :p95 (nth sorted-times (int (* n 0.95)))
     :bytes bytes}))

(defn- warm-up-mode-specs
  [schema rows mode-specs config]
  (loop [iteration-idx
         0]
    (when (< iteration-idx (:warmup-iterations config))
      (let [ordered-mode-specs
            (shuffle-mode-specs mode-specs
                                (:order-seed config)
                                iteration-idx)]
        (doseq [mode-spec
                ordered-mode-specs]
          (write-once schema rows mode-spec config))
        (recur (unchecked-inc iteration-idx))))))

(defn- bench-mode-specs
  [schema rows mode-specs config]
  (warm-up-mode-specs schema rows mode-specs config)
  (loop [iteration-idx
         0

         samples
         (empty-samples mode-specs)]
    (if (< iteration-idx (:bench-iterations config))
      (let [ordered-mode-specs
            (shuffle-mode-specs mode-specs
                                (:order-seed config)
                                (+ (:warmup-iterations config)
                                   iteration-idx))

            samples
            (reduce (fn [samples mode-spec]
                      (let [[result time-ms]
                            (measure-time-ms (fn []
                                               (write-once schema
                                                           rows
                                                           mode-spec
                                                           config)))

                            byte-size
                            (alength ^bytes result)]
                        (add-sample samples
                                    (:label mode-spec)
                                    time-ms
                                    byte-size)))
                    samples
                    ordered-mode-specs)]
        (recur (unchecked-inc iteration-idx)
               samples))
      (mapv (fn [mode-spec]
              {:label (:label mode-spec)
               :stats (sample-stats (get samples (:label mode-spec)))})
            mode-specs))))

(defn- bench-single-mode
  [schema rows mode-spec config]
  (let [results
        (bench-mode-specs schema rows [mode-spec] config)]
    (first results)))

(defn- percent-delta
  [baseline current]
  (if (zero? baseline)
    0.0
    (* 100.0 (/ (- current baseline) baseline))))

;;; Printing

(defn- print-header
  [title config]
  (println "\n")
  (println "====================================")
  (println (str "  " title))
  (println "====================================")
  (println (str "Date: " (java.time.LocalDateTime/now)))
  (println (str "JVM: " (System/getProperty "java.version")))
  (println (str "Threads: " (:threads config)))
  (println (str "Compression: " (:compression config)))
  (println (str "Data seed: " (:data-seed config)))
  (println (str "Order seed: " (:order-seed config)))
  (when-let [row-counts
             (:row-counts config)]
    (println (str "Row counts: " row-counts))))

(defn- print-footer
  [title]
  (println "\n====================================")
  (println (str "  " title))
  (println "====================================\n"))

(defn- print-mode-results
  [row-count results]
  (println (format "\n  %d rows" row-count))
  (let [baseline-mean
        (get-in (first results) [:stats :mean])]
    (doseq [{:keys [label stats]}
            results]
      (let [mean
            (:mean stats)

            delta-pct
            (percent-delta baseline-mean mean)

            delta-str
            (if (= label "counted")
              "baseline"
              (format "%+.1f%% vs counted" delta-pct))]
        (println
         (format "    %-24s min=%8.2fms mean=%8.2fms median=%8.2fms p95=%8.2fms max=%8.2fms rows/ms=%8.2f bytes=%10d  %s"
                 label
                 (:min stats)
                 mean
                 (:median stats)
                 (:p95 stats)
                 (:max stats)
                 (/ row-count mean)
                 (:bytes stats)
                 delta-str))))))

(defn- print-best-result
  [results]
  (let [sorted-results
        (sort-by (fn [{:keys [stats]}]
                   (:mean stats))
                 results)

        best-result
        (first sorted-results)]
    (println
     (format "    best: %s at %.2fms"
             (:label best-result)
             (get-in best-result [:stats :mean])))))

(defn- print-single-mode-result
  [scenario row-count result]
  (let [stats
        (:stats result)]
    (println
     (format "%-14s %9d rows  %-24s min=%8.2fms mean=%8.2fms median=%8.2fms p95=%8.2fms max=%8.2fms rows/ms=%8.2f bytes=%10d"
             (name (:key scenario))
             row-count
             (:label result)
             (:min stats)
             (:mean stats)
             (:median stats)
             (:p95 stats)
             (:max stats)
             (/ row-count (:mean stats))
             (:bytes stats)))))

;;; Run modes

(defn- run-compare-scenario
  [scenario config]
  (println (str "\n=== " (:name scenario) " ==="))
  (doseq [row-count
          (:row-counts config)]
    (let [rows
          (generate-rows (:key scenario)
                         row-count
                         (:data-seed config))

          results
          (bench-mode-specs (:schema scenario)
                            rows
                            (compare-mode-specs (:threads config))
                            config)]
      (print-mode-results row-count results))))

(defn run-compare
  []
  (let [config
        (compare-config)

        selected-scenarios
        (select-scenarios (:scenario-keys config))]
    (print-header "Parquet Writer Compare Benchmark" config)
    (doseq [scenario
            selected-scenarios]
      (run-compare-scenario scenario config))
    (print-footer "Compare Benchmark Complete")))

(defn run-quick
  []
  (let [config
        (quick-config)

        selected-scenarios
        (select-scenarios (:scenario-keys config))]
    (print-header "Parquet Writer Quick Compare" config)
    (doseq [scenario
            selected-scenarios]
      (run-compare-scenario scenario config))
    (print-footer "Quick Compare Complete")))

(defn- run-tune-scenario
  [scenario config]
  (println (str "\n=== " (:name scenario) " ==="))
  (doseq [row-count
          (:row-counts config)]
    (let [rows
          (generate-rows (:key scenario)
                         row-count
                         (:data-seed config))

          mode-specs
          (tuned-lazy-mode-specs (:threads config)
                                 (:chunk-sizes config)
                                 (:max-in-flight-chunks config))

          results
          (bench-mode-specs (:schema scenario)
                            rows
                            mode-specs
                            config)]
      (print-mode-results row-count results)
      (print-best-result results))))

(defn run-tune
  []
  (let [config
        (tune-config)

        selected-scenarios
        (select-scenarios (:scenario-keys config))]
    (print-header "Parquet Writer Lazy Tuning Benchmark" config)
    (println (str "Chunk sizes: " (:chunk-sizes config)))
    (println (str "Max in flight: " (:max-in-flight-chunks config)))
    (doseq [scenario
            selected-scenarios]
      (run-tune-scenario scenario config))
    (print-footer "Lazy Tuning Benchmark Complete")))

(defn run-one
  [scenario-key row-count mode-kind & [chunk-size max-in-flight-chunks]]
  (let [config
        (one-config)

        scenario
        (lookup-scenario (keyword scenario-key))

        row-count
        (parse-long-value row-count)

        rows
        (generate-rows (:key scenario)
                       row-count
                       (:data-seed config))

        mode-spec
        (mode-spec-from-cli (:threads config)
                            mode-kind
                            chunk-size
                            max-in-flight-chunks)

        result
        (bench-single-mode (:schema scenario)
                           rows
                           mode-spec
                           config)]
    (print-single-mode-result scenario row-count result)))

(defn run-all
  []
  (run-compare)
  (run-tune))

(defn quick-bench
  []
  (run-quick))

(defn -main
  [& args]
  (let [command
        (or (first args) "compare")

        command-args
        (rest args)]
    (case command
      "compare" (run-compare)
      "quick" (run-quick)
      "tune" (run-tune)
      "all" (run-all)
      "one" (apply run-one command-args)
      (do
        (println "Unknown command:" command)
        (println "Usage: ./profiler/run_bench.sh [compare|quick|tune|all|one]")
        (println "       ./profiler/run_bench.sh one <scenario-key> <row-count> <counted|lazy-default|lazy-single|lazy-custom> [chunk-size] [max-in-flight-chunks]")
        (System/exit 1))))
  (shutdown-agents))

(comment
  (run-compare)
  (run-quick)
  (run-tune)
  (run-one "mixed" "2000000" "lazy-default")
  (run-one "mixed" "2000000" "lazy-custom" "50000" "12"))
