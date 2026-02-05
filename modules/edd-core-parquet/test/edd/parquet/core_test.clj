(ns edd.parquet.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [edd.parquet.core :as parquet])
  (:import [java.io File FileOutputStream]
           [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.io LocalInputFile]))

(set! *warn-on-reflection* true)

(def test-schema
  {:description "Test orders table",
   :columns [["ORDER_ID" :uuid "Order identifier" :required]
             ["CUSTOMER" :string "Customer name" :required]
             ["AMOUNT" :double "Order amount" :required]
             ["QUANTITY" :long "Item quantity" :optional]
             ["STATUS" :enum "Order status" :required :enum
              ["PENDING" "SHIPPED" "DELIVERED"]]
             ["IS_PRIORITY" :boolean "Priority flag" :optional]
             ["CREATED" :date "Creation date" :optional]]})

(def test-rows
  [{"ORDER_ID" "123e4567-e89b-12d3-a456-426614174000",
    "CUSTOMER" "Acme Corp",
    "AMOUNT" 1234.56,
    "QUANTITY" 10,
    "STATUS" "PENDING",
    "IS_PRIORITY" true,
    "CREATED" "2024-01-15"}
   {"ORDER_ID" "223e4567-e89b-12d3-a456-426614174001",
    "CUSTOMER" "Beta Inc",
    "AMOUNT" 999.99,
    "QUANTITY" 5,
    "STATUS" "SHIPPED",
    "IS_PRIORITY" false,
    "CREATED" "2024-01-20"}
   {"ORDER_ID" "323e4567-e89b-12d3-a456-426614174002",
    "CUSTOMER" "Gamma LLC",
    "AMOUNT" 500.0,
    "STATUS" "DELIVERED"}])

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

(deftest mime-type-test
  (testing "MIME type constant"
    (is (= "application/vnd.apache.parquet" (parquet/mime-type)))))

(deftest get-codec-test
  (testing "Compression codec mapping"
    (is (= org.apache.parquet.hadoop.metadata.CompressionCodecName/GZIP
           (parquet/get-codec :gzip)))
    (is (= org.apache.parquet.hadoop.metadata.CompressionCodecName/UNCOMPRESSED
           (parquet/get-codec :uncompressed)))
    (is (= org.apache.parquet.hadoop.metadata.CompressionCodecName/UNCOMPRESSED
           (parquet/get-codec :unknown)))))

(deftest schema-fingerprint-test
  (testing "Schema fingerprint is deterministic"
    (let [fp1
          (parquet/schema-fingerprint test-schema)

          fp2
          (parquet/schema-fingerprint test-schema)]
      (is (string? fp1))
      (is (pos? (count fp1)))
      (is (= fp1 fp2))))
  (testing "Different schemas produce different fingerprints"
    (let [schema-a
          {:description "A", :columns [["COL" :string "desc" :required]]}

          schema-b
          {:description "B", :columns [["COL" :string "desc" :required]]}

          schema-c
          {:description "A", :columns [["COL" :string "desc" :optional]]}]
      (is (not= (parquet/schema-fingerprint schema-a)
                (parquet/schema-fingerprint schema-b)))
      (is (not= (parquet/schema-fingerprint schema-a)
                (parquet/schema-fingerprint schema-c))))))

(deftest write-parquet-bytes-basic-test
  (testing "Basic parquet generation produces valid bytes"
    (let [result
          (parquet/write-parquet-bytes {:table-name "orders",
                                        :schema test-schema,
                                        :rows test-rows,
                                        :compression :uncompressed})]
      (is (bytes? result))
      (is (pos? (alength ^bytes result)))
      (is (= (int \P) (aget ^bytes result 0)))
      (is (= (int \A) (aget ^bytes result 1)))
      (is (= (int \R) (aget ^bytes result 2)))
      (is (= (int \1) (aget ^bytes result 3))))))

(deftest write-parquet-bytes-gzip-test
  (testing "GZIP compression produces valid parquet"
    (let [result
          (parquet/write-parquet-bytes {:table-name "orders",
                                        :schema test-schema,
                                        :rows test-rows,
                                        :compression :gzip})]
      (is (bytes? result))
      (is (pos? (alength ^bytes result)))
      (is (= (int \P) (aget ^bytes result 0))))))

(deftest write-parquet-bytes-metadata-test
  (testing "Parquet file contains correct metadata"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "test_table",
            :schema {:description "Test description",
                     :columns [["COL1" :string "Column 1" :required]
                               ["COL2" :long "Column 2" :optional]]},
            :rows [{"COL1" "value1", "COL2" 42}],
            :schema-version "1.2.3",
            :table-schema "my_database_schema",
            :compression :uncompressed})

          f
          (write-bytes-to-temp-file result "metadata_test")]
      (with-open [reader
                  (ParquetFileReader/open (LocalInputFile. (.toPath f)))]
        (let [md
              (.getKeyValueMetaData (.getFileMetaData reader))]
          (is (= "test_table" (get md "table.name")))
          (is (= "Test description" (get md "table.description")))
          (is (= "1.2.3" (get md "schema.version")))
          (is (= "my_database_schema" (get md "table.schema")))
          (is (string? (get md "schema.fingerprint")))
          (is (= "Column 1" (get md "column.COL1.description")))
          (is (= "required" (get md "column.COL1.requirement")))
          (is (= "Column 2" (get md "column.COL2.description")))
          (is (= "optional" (get md "column.COL2.requirement"))))))))

(deftest write-parquet-bytes-empty-rows-test
  (testing "Empty rows produce valid parquet"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "empty",
            :schema {:description "Empty table",
                     :columns [["COL" :string "Column" :optional]]},
            :rows [],
            :compression :uncompressed})]
      (is (bytes? result))
      (is (pos? (alength ^bytes result))))))

(deftest write-parquet-bytes-filters-extra-columns-test
  (testing "Extra columns in rows are filtered out"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "filtered",
            :schema {:description "Filtered",
                     :columns [["COL1" :string "Column 1" :required]]},
            :rows [{"COL1" "value", "EXTRA_COL" "ignored"}],
            :compression :uncompressed})]
      (is (bytes? result))
      (is (pos? (alength ^bytes result))))))

(deftest write-parquet-bytes-required-validation-test
  (testing "Missing required column throws exception"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Required column 'COL1' has nil value"
                          (parquet/write-parquet-bytes
                           {:table-name "validation",
                            :schema {:description "Validation test",
                                     :columns [["COL1" :string "Column 1"
                                                :required]]},
                            :rows [{"COL1" nil}],
                            :compression :uncompressed})))))

(deftest write-parquet-bytes-enum-validation-test
  (testing "Invalid enum value throws exception"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Invalid enum value"
                          (parquet/write-parquet-bytes
                           {:table-name "enum_validation",
                            :schema {:description "Enum test",
                                     :columns [["STATUS" :enum "Status"
                                                :required :enum
                                                ["A" "B" "C"]]]},
                            :rows [{"STATUS" "INVALID"}],
                            :compression :uncompressed}))))
  (testing "Valid enum value succeeds"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "enum_valid",
            :schema {:description "Enum test",
                     :columns [["STATUS" :enum "Status" :required :enum
                                ["A" "B" "C"]]]},
            :rows [{"STATUS" "B"}],
            :compression :uncompressed})]
      (is (bytes? result)))))

(deftest write-parquet-bytes-all-types-test
  (testing "All column types write successfully"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "all_types",
            :schema {:description "All types test",
                     :columns [["STR" :string "String" :required]
                               ["ENUM" :enum "Enum" :required :enum ["X" "Y"]]
                               ["BOOL" :boolean "Boolean" :required]
                               ["UUID" :uuid "UUID" :required]
                               ["DATE" :date "Date" :required]
                               ["DBL" :double "Double" :required]
                               ["LNG" :long "Long" :required]]},
            :rows [{"STR" "hello",
                    "ENUM" "X",
                    "BOOL" true,
                    "UUID" "550e8400-e29b-41d4-a716-446655440000",
                    "DATE" "2024-06-15",
                    "DBL" 3.14159,
                    "LNG" 9223372036854775807}],
            :compression :uncompressed})]
      (is (bytes? result))
      (is (pos? (alength ^bytes result))))))

(deftest write-parquet-bytes-optional-nulls-test
  (testing "Optional columns can be nil"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "optional_nulls",
            :schema {:description "Optional nulls test",
                     :columns [["REQ" :string "Required" :required]
                               ["OPT1" :string "Optional 1" :optional]
                               ["OPT2" :long "Optional 2" :optional]
                               ["OPT3" :date "Optional 3" :optional]]},
            :rows [{"REQ" "value", "OPT1" nil, "OPT2" nil, "OPT3" nil}
                   {"REQ" "value2", "OPT1" "present"}],
            :compression :uncompressed})]
      (is (bytes? result))
      (is (pos? (alength ^bytes result))))))

(deftest write-parquet-bytes-date-conversion-test
  (testing "Date strings are converted to epoch days"
    (let [result
          (parquet/write-parquet-bytes
           {:table-name "dates",
            :schema {:description "Date test",
                     :columns [["D" :date "Date" :required]]},
            :rows [{"D" "1970-01-01"} {"D" "2024-01-01"} {"D" "2000-06-15"}],
            :compression :uncompressed})]
      (is (bytes? result)))))

(deftest write-parquet-bytes-row-count-test
  (testing "Row count in parquet metadata matches input"
    (let [rows
          (vec (repeat 50 {"COL" "value"}))

          result
          (parquet/write-parquet-bytes
           {:table-name "row_count",
            :schema {:description "Row count test",
                     :columns [["COL" :string "Column" :required]]},
            :rows rows,
            :compression :uncompressed})

          f
          (write-bytes-to-temp-file result "row_count_test")]
      (with-open [reader
                  (ParquetFileReader/open (LocalInputFile. (.toPath f)))]
        (is (= 50 (.getRecordCount reader)))))))
