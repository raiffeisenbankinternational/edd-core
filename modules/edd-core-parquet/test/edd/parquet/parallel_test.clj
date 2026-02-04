(ns edd.parquet.parallel-test
  (:require [clojure.test :refer [deftest is testing]]
            [edd.parquet.core :as parquet])
  (:import [java.io File FileOutputStream]
           [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.io LocalInputFile]))

(set! *warn-on-reflection* true)

(def merge-test-schema
  {:description "Merge test table",
   :columns [["ID" :long "ID" :required] ["NAME" :string "Name" :required]]})

(defn- write-bytes-to-temp-file
  "Writes bytes to a temp file and returns the File."
  ^File [^bytes data prefix]
  (let [f
        (File/createTempFile prefix ".parquet")]
    (.deleteOnExit f)
    (with-open [out
                (FileOutputStream. f)]
      (.write out data))
    f))

(defn- get-parquet-row-count
  "Reads a parquet file and returns the record count."
  [^File file]
  (with-open [reader
              (ParquetFileReader/open (LocalInputFile. (.toPath file)))]
    (.getRecordCount reader)))

(deftest partition-rows-empty-seq-test
  (testing "partition-rows with empty seq returns empty vector"
    (is (= [] (parquet/partition-rows [] 4)))))

(deftest partition-rows-single-item-test
  (testing "partition-rows with single item returns single partition"
    (is (= [[1]] (parquet/partition-rows [1] 4)))))

(deftest partition-rows-fewer-items-than-threads-test
  (testing
   "partition-rows with N items < thread-count returns appropriate partitions"
    (let [result
          (parquet/partition-rows [1 2 3] 4)]
      (is (vector? result))
      (is (every? vector? result))
      (is (<= (count result) 3)))))

(deftest partition-rows-more-items-than-threads-test
  (testing
   "partition-rows with N items > thread-count returns thread-count partitions"
    (let [rows
          (range 100)

          result
          (parquet/partition-rows rows 4)]
      (is (= 4 (count result)))
      (is (every? vector? result)))))

(deftest partition-rows-preserves-all-rows-test
  (testing "partition-rows preserves all rows without data loss"
    (let [rows
          [{"id" 1} {"id" 2} {"id" 3} {"id" 4} {"id" 5}]

          result
          (parquet/partition-rows rows 2)

          flattened
          (vec (mapcat identity result))]
      (is (= rows flattened)))))

(deftest partition-rows-single-thread-test
  (testing
   "partition-rows with :threads 1 returns single partition containing all items"
    (let [rows
          [1 2 3 4 5 6]

          result
          (parquet/partition-rows rows 1)]
      (is (= 1 (count result)))
      (is (= [rows] result)))))

(deftest partition-rows-balanced-distribution-test
  (testing "partition-rows distributes rows relatively evenly across threads"
    (let [rows
          (range 25)

          result
          (parquet/partition-rows rows 4)

          partition-sizes
          (map count result)

          min-size
          (apply min partition-sizes)

          max-size
          (apply max partition-sizes)]
      (is (<= (- max-size min-size) 1)))))

(defn- get-parquet-metadata
  "Reads a parquet file and returns the key-value metadata map."
  [^File file]
  (with-open [reader
              (ParquetFileReader/open (LocalInputFile. (.toPath file)))]
    (.getKeyValueMetaData (.getFileMetaData reader))))

;;; ============================================================================
;;; write-parquet-bytes parallel tests
;;; ============================================================================

(deftest write-parquet-bytes-parallel-mem-empty-rows-test
  (testing
   "write-parquet-bytes-parallel-mem handles empty rows and returns valid parquet"
    (let [bytes
          (parquet/write-parquet-bytes {:table-name "parallel_mem_empty",
                                        :schema merge-test-schema,
                                        :rows [],
                                        :compression :uncompressed,
                                        :threads 2})

          file
          (write-bytes-to-temp-file bytes "parallel_mem_empty")]
      (is (bytes? bytes))
      (is (= (byte 0x50) (aget ^bytes bytes 0)))
      (is (= 0 (get-parquet-row-count file))))))

(deftest write-parquet-bytes-parallel-mem-large-dataset-test
  (testing "write-parquet-bytes handles large datasets above parallel threshold"
    (let [;; Generate 15,000 rows to exceed the 10,000 threshold

          large-rows
          (mapv (fn [i] {"ID" (long i), "NAME" (str "Name-" i)}) (range 15000))

          bytes
          (parquet/write-parquet-bytes {:table-name "parallel_mem_large",
                                        :schema merge-test-schema,
                                        :rows large-rows,
                                        :compression :uncompressed,
                                        :table-schema "table.schema",
                                        :threads 4})

          file
          (write-bytes-to-temp-file bytes "parallel_mem_large")

          md
          (get-parquet-metadata file)]
      (is (bytes? bytes))
      (is (= 15000 (get-parquet-row-count file)))
      (is (= "table.schema" (get md "table.schema"))))))

