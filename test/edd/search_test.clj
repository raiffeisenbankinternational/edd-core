(ns edd.search-test
  (:require [clojure.test :refer :all]
            [edd.search :as search]))

(deftest sort-forming
  (is (= [{"a.keyword" {:order "asc"}}
          {"b.keyword" {:order "desc"}}
          {"c.number" {:order "asc"}}
          {"d.number" {:order "desc"}}]
         (search/form-sorting
          [:a :asc
           :b :desc
           :c :asc-number
           :d :desc-number]))))
