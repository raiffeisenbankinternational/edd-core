(ns edd.xlsx.core-test
  (:import
   java.io.File
   java.time.LocalDate)
  (:require
   [clojure.test :refer [deftest is]]
   [edd.xlsx.core :as xlsx]))

(deftest test-xlsx-file-encode-decode
  (let [file
        (File/createTempFile "test" ".xlsx")]

    (xlsx/write-matrix [["Title" nil :test ""]
                        ["German Umlauts" "ä" "ö" "ü"]
                        [1 1.2 #uuid "921e5bd7-4575-41d3-b3c0-071491c98f13"]
                        [(LocalDate/parse "2024-12-31")]]
                       "title"
                       file)

    (let [data (xlsx/read-matrix file)]
      (is (= [["Title" nil "test" ""]
              ["German Umlauts" "ä" "ö" "ü"]
              ["1" "1.2" "921e5bd7-4575-41d3-b3c0-071491c98f13"]
              ["2024-12-31"]]
             data)))))
