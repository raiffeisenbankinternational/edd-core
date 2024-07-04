(ns edd.xlsx.core-test
  (:import
   java.io.File
   java.time.LocalDate)
  (:require
   [clojure.test :refer [deftest is]]
   [edd.xlsx.core :as xlsx]))

(deftest test-xlsx-file-encode-decode
  (let [path (str (File/createTempFile "test" ".xlsx"))]

    (xlsx/write-matrix-to-file [["Title" nil :test ""]
                                ["German Umlauts" "ä" "ö" "ü"]
                                [1 1.2]
                                [(LocalDate/parse "2024-12-31")]]
                               "title"
                               path)

    (let [data (xlsx/read-matrix-from-file path)]
      (is (= [["Title" nil ":test" ""]
              ["German Umlauts" "ä" "ö" "ü"]
              ["1" "1.2"]
              ["2024-12-31"]]
             data)))))
