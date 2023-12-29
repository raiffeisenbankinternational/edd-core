(ns batch.csv-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [batch.csv :as csv]
            [lambda.util :as util]))

(def columns
  [" a" "a b" "a_b" " a b _c"])

(deftest test-column-converter
  (is (= [:a :a-b :a-b :a-b--c]
         (into [] (map
                   #(csv/convert-column-names % false)
                   columns)))))

(deftest test-parser
  (let [result (csv/parse-csv
                (io/reader (io/resource "batch/csv-test.csv")))]
    (is (= {:b "2" :a "1"}
           (first (into [] result))))))

(deftest test-parser-comma
  (let [result (csv/parse-csv
                (io/reader (io/resource "batch/csv-test-comma.csv"))
                \,)]
    (is (= {:b "2" :a "1"}
           (first (into [] result))))))

(deftest test-parser-original
  (let [result (csv/parse-csv
                (io/reader (io/resource "batch/csv-test.csv"))
                \;
                true)]
    (is (= {:A "1" :b "2"}
           (first (into [] result))))))

(deftest test-parser-broken
  (let [result (csv/parse-csv
                (io/reader (io/resource "batch/broken.csv"))
                \;
                true)]
    (is (= {:C1 "a"
            :C2 "Lorem \"\"Ipsum\"\" \"\"b"}
           (first (into [] result))))))

(deftest test-parser-newline-only
  (let [result (csv/parse-csv
                (io/reader (io/resource "batch/csv-test-newline.csv")))]
    (is (= nil
           (first (into [] result))))))

(deftest test-serialize-reader
  (is (= "{\"a\":\":b\",\"c\":\"BufferedReader\"}"
         (util/to-json {:a :b :c (io/reader (io/resource "batch/csv-test.csv"))}))))
