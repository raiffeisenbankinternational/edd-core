(ns edd.parquet.lazy-parallel-test
  (:require [clojure.test :refer [deftest is testing]]
            [edd.parquet.core :as parquet])
  (:import [java.io File FileOutputStream]
           [org.apache.parquet.example.data.simple SimpleGroup]
           [org.apache.parquet.example.data.simple.convert GroupRecordConverter]
           [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.io ColumnIOFactory LocalInputFile]))

(set! *warn-on-reflection* true)

(def lazy-test-schema
  {:description "Lazy parallel test table",
   :columns [["ID" :long "ID" :required]
             ["NAME" :string "Name" :required]]})

(defn- write-bytes-to-temp-file
  "Writes bytes to a temp file and returns File."
  ^File [^bytes data prefix]
  (let [file
        (File/createTempFile prefix ".parquet")]
    (.deleteOnExit file)
    (with-open [out
                (FileOutputStream. file)]
      (.write out data))
    file))

(defn- get-parquet-row-count
  [^File file]
  (with-open [reader
              (ParquetFileReader/open (LocalInputFile. (.toPath file)))]
    (.getRecordCount reader)))

(defn- get-parquet-metadata
  [^File file]
  (with-open [reader
              (ParquetFileReader/open (LocalInputFile. (.toPath file)))]
    (.getKeyValueMetaData (.getFileMetaData reader))))

(defn- read-parquet-rows
  [^File file]
  (with-open [reader
              (ParquetFileReader/open (LocalInputFile. (.toPath file)))]
    (let [file-schema
          (.getSchema (.getFileMetaData reader))

          column-io
          (.getColumnIO (ColumnIOFactory.) file-schema)]
      (loop [rows
             []]
        (if-let [page-store
                 (.readNextRowGroup reader)]
          (let [record-converter
                (GroupRecordConverter. file-schema)

                record-reader
                (.getRecordReader column-io page-store record-converter)

                row-count
                (.getRowCount page-store)]
            (recur (loop [rows
                          rows

                          row-idx
                          0]
                     (if (< row-idx row-count)
                       (let [^SimpleGroup group
                             (.read record-reader)]
                         (recur (conj rows {"ID" (.getLong group "ID" 0)
                                            "NAME" (.getString group "NAME" 0)})
                                (unchecked-inc row-idx)))
                       rows))))
          rows)))))

(defn- lazy-rows
  [n]
  (map (fn [i]
         {"ID" (long i)
          "NAME" (str "Name-" i)})
       (range n)))

(defn- exploding-rows
  [fail-at]
  ((fn step [idx]
     (lazy-seq
      (cond
        (< idx fail-at)
        (cons {"ID" (long idx)
               "NAME" (str "Name-" idx)}
              (step (inc idx)))

        (= idx fail-at)
        (throw (ex-info "Lazy row generation failed"
                        {:idx idx}))

        :else
        nil)))
   0))

(deftest write-parquet-bytes-lazy-parallel-small-input-test
  (testing "lazy-parallel writes small lazy input and preserves metadata"
    (let [expected-rows
          [{"ID" 0 "NAME" "Name-0"}
           {"ID" 1 "NAME" "Name-1"}
           {"ID" 2 "NAME" "Name-2"}]

          bytes
          (parquet/write-parquet-bytes-lazy-parallel
           {:table-name "lazy_small",
            :schema lazy-test-schema,
            :rows (map identity expected-rows),
            :compression :uncompressed,
            :schema-version "2.0",
            :table-schema "lazy.schema",
            :threads 2,
            :chunk-size 10,
            :max-in-flight-chunks 2})

          file
          (write-bytes-to-temp-file bytes "lazy_small")

          metadata
          (get-parquet-metadata file)]
      (is
       (bytes? bytes))

      (is
       (= expected-rows
          (read-parquet-rows file)))

      (is
       (= "lazy_small"
          (get metadata "table.name")))

      (is
       (= "2.0"
          (get metadata "schema.version")))

      (is
       (= "lazy.schema"
          (get metadata "table.schema"))))))

(deftest write-parquet-bytes-lazy-parallel-multi-chunk-order-test
  (testing "lazy-parallel preserves row order across out-of-order chunk completion"
    (let [expected-rows
          (vec (lazy-rows 40))

          original-write-chunk-to-bytes
          @#'edd.parquet.core/write-chunk-to-bytes

          bytes
          (with-redefs-fn
            {#'edd.parquet.core/write-chunk-to-bytes
             (fn [{:keys [rows] :as opts}]
               (let [first-id
                     (get (first rows) "ID")

                     delay-ms
                     (cond
                       (= 0 first-id) 120
                       (= 10 first-id) 90
                       (= 20 first-id) 60
                       (= 30 first-id) 30
                       :else 0)]
                 (when (pos? delay-ms)
                   (Thread/sleep (long delay-ms)))
                 (original-write-chunk-to-bytes opts)))}
            #(parquet/write-parquet-bytes-lazy-parallel
              {:table-name "lazy_multi_chunk",
               :schema lazy-test-schema,
               :rows (lazy-rows 40),
               :compression :uncompressed,
               :threads 4,
               :chunk-size 10,
               :max-in-flight-chunks 4}))

          file
          (write-bytes-to-temp-file bytes "lazy_multi_chunk")]
      (is
       (= expected-rows
          (read-parquet-rows file)))

      (is
       (= 40
          (get-parquet-row-count file))))))

(deftest write-parquet-bytes-lazy-parallel-empty-rows-test
  (testing "lazy-parallel handles empty lazy rows"
    (let [bytes
          (parquet/write-parquet-bytes-lazy-parallel
           {:table-name "lazy_empty",
            :schema lazy-test-schema,
            :rows (map identity []),
            :compression :uncompressed,
            :threads 2,
            :chunk-size 5})

          file
          (write-bytes-to-temp-file bytes "lazy_empty")]
      (is
       (bytes? bytes))

      (is
       (= 0
          (get-parquet-row-count file))))))

(deftest write-parquet-bytes-lazy-parallel-single-chunk-test
  (testing "lazy-parallel works when all rows fit in single chunk"
    (let [expected-rows
          (vec (lazy-rows 5))

          bytes
          (parquet/write-parquet-bytes-lazy-parallel
           {:table-name "lazy_single_chunk",
            :schema lazy-test-schema,
            :rows (lazy-rows 5),
            :compression :uncompressed,
            :threads 3,
            :chunk-size 100,
            :max-in-flight-chunks 3})

          file
          (write-bytes-to-temp-file bytes "lazy_single_chunk")]
      (is
       (= expected-rows
          (read-parquet-rows file))))))

(deftest write-parquet-bytes-lazy-parallel-row-generation-error-test
  (testing "lazy-parallel surfaces lazy row generation errors"
    (is
     (thrown-with-msg?
      clojure.lang.ExceptionInfo
      #"Lazy row generation failed"
      (parquet/write-parquet-bytes-lazy-parallel
       {:table-name "lazy_generation_error",
        :schema lazy-test-schema,
        :rows (exploding-rows 3),
        :compression :uncompressed,
        :threads 2,
        :chunk-size 2,
        :max-in-flight-chunks 2})))))

(deftest write-parquet-bytes-lazy-parallel-worker-error-test
  (testing "lazy-parallel surfaces worker chunk write errors"
    (is
     (thrown-with-msg?
      clojure.lang.ExceptionInfo
      #"Required column 'NAME' in table-schema 'lazy.schema' has nil value"
      (parquet/write-parquet-bytes-lazy-parallel
       {:table-name "lazy_worker_error",
        :schema lazy-test-schema,
        :table-schema "lazy.schema",
        :rows (concat [{"ID" 0 "NAME" "Name-0"}
                       {"ID" 1 "NAME" "Name-1"}]
                      [{"ID" 2 "NAME" nil}
                       {"ID" 3 "NAME" "Name-3"}]),
        :compression :uncompressed,
        :threads 2,
        :chunk-size 2,
        :max-in-flight-chunks 2})))))
