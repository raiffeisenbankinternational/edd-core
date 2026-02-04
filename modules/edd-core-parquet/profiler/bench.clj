(ns bench
  "Benchmarks for parquet writer performance.
   
   Run with: ./profiler/run_bench.sh"
  (:require [edd.parquet.core :as parquet]))

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

;;; Row Generators

(defn- random-uuid-str []
  (str (java.util.UUID/randomUUID)))

(defn- generate-mixed-row [^long i]
  {"ID" (random-uuid-str)
   "NAME" (str "Customer-" i)
   "AMOUNT" (* (rand) 10000.0)
   "QUANTITY" (long (rand-int 1000))
   "STATUS" (rand-nth ["PENDING" "ACTIVE" "CLOSED"])
   "IS_ACTIVE" (rand-nth [true false nil])
   "CREATED" "2024-01-15"})

(defn- generate-string-row [^long i]
  {"ID" (random-uuid-str)
   "NAME" (str "Customer-" i "-" (random-uuid-str))
   "DESCRIPTION" (str "This is a longer description for item " i " with some additional text")
   "CATEGORY" (str "Category-" (mod i 100))
   "NOTES" (when (zero? (mod i 3)) (str "Notes for " i))})

(defn- generate-numeric-row [^long i]
  {"ID" i
   "AMOUNT1" (* (rand) 10000.0)
   "AMOUNT2" (* (rand) 5000.0)
   "AMOUNT3" (* (rand) 1000.0)
   "COUNT1" (long (rand-int 10000))
   "COUNT2" (long (rand-int 5000))})

(defn- generate-enum-row [^long _i]
  {"ID" (random-uuid-str)
   "STATUS" (rand-nth ["PENDING" "ACTIVE" "CLOSED" "CANCELLED"])
   "TYPE" (rand-nth ["A" "B" "C" "D" "E"])
   "REGION" (rand-nth ["NORTH" "SOUTH" "EAST" "WEST"])
   "PRIORITY" (rand-nth ["LOW" "MEDIUM" "HIGH" "CRITICAL"])})

(defn generate-rows
  "Generate n rows using the given generator function"
  [n generator]
  (mapv generator (range n)))

;;; Benchmark Utilities

(defn- warm-up
  "Warm up JIT by running function multiple times"
  [f n]
  (dotimes [_ n]
    (f)))

(defn- measure-time-ms
  "Measure execution time in milliseconds, returns [result time-ms]"
  [f]
  (let [start (System/nanoTime)
        result (f)
        end (System/nanoTime)]
    [result (/ (- end start) 1000000.0)]))

(defn- run-iterations
  "Run function n times and return timing statistics"
  [f iterations]
  (let [times (mapv (fn [_] (second (measure-time-ms f))) (range iterations))
        sorted-times (sort times)
        n (count sorted-times)]
    {:min (first sorted-times)
     :max (last sorted-times)
     :mean (/ (reduce + times) n)
     :median (nth sorted-times (quot n 2))
     :p95 (nth sorted-times (int (* n 0.95)))}))

;;; Benchmarks

(defn bench-write
  "Benchmark write-parquet-bytes with given schema and rows.
   Returns timing statistics in milliseconds."
  [schema rows & {:keys [warmup-iterations bench-iterations compression]
                  :or {warmup-iterations 5
                       bench-iterations 20
                       compression :gzip}}]
  (let [write-fn (fn []
                   (parquet/write-parquet-bytes
                    {:table-name "bench"
                     :schema schema
                     :rows rows
                     :compression compression}))]
    ;; Warm up
    (warm-up write-fn warmup-iterations)
    ;; Benchmark
    (run-iterations write-fn bench-iterations)))

(defn bench-scenario
  "Run benchmark for a specific scenario and print results"
  [name schema row-generator row-counts]
  (println (str "\n=== " name " ==="))
  (doseq [n row-counts]
    (let [rows (generate-rows n row-generator)
          stats (bench-write schema rows)]
      (println (format "  %6d rows: min=%.2fms mean=%.2fms median=%.2fms p95=%.2fms max=%.2fms (%.2f rows/ms)"
                       n
                       (:min stats)
                       (:mean stats)
                       (:median stats)
                       (:p95 stats)
                       (:max stats)
                       (/ n (:mean stats)))))))

(defn run-all
  "Run all benchmark scenarios"
  []
  (let [row-counts [100 1000 5000 10000]]
    (println "\n")
    (println "====================================")
    (println "  Parquet Writer Benchmark Suite")
    (println "====================================")
    (println (str "Date: " (java.time.LocalDateTime/now)))
    (println (str "JVM: " (System/getProperty "java.version")))
    (println (str "Row counts: " row-counts))

    (bench-scenario "Mixed Schema (typical usage)"
                    schema-mixed generate-mixed-row row-counts)

    (bench-scenario "String Heavy Schema"
                    schema-string-heavy generate-string-row row-counts)

    (bench-scenario "Numeric Heavy Schema"
                    schema-numeric-heavy generate-numeric-row row-counts)

    (bench-scenario "Enum Heavy Schema"
                    schema-enum-heavy generate-enum-row row-counts)

    (println "\n====================================")
    (println "  Benchmark Complete")
    (println "====================================\n")
    (shutdown-agents)))

(defn quick-bench
  "Quick benchmark with fewer rows for rapid testing"
  []
  (let [row-counts [100 1000]]
    (println "\n=== Quick Benchmark ===")
    (bench-scenario "Mixed Schema" schema-mixed generate-mixed-row row-counts)
    (bench-scenario "Enum Heavy" schema-enum-heavy generate-enum-row row-counts)))

(comment
  ;; Run quick benchmark
  (quick-bench)

  ;; Run full benchmark
  (run-all)

  ;; Benchmark specific scenario
  (bench-scenario "Custom"
                  schema-mixed
                  generate-mixed-row
                  [100 500 1000]))
