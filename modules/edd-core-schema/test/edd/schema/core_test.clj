(ns edd.schema.core-test
  (:require [edd.schema.core :as sut]
            [clojure.test.check.properties :as prop]
            [malli.core :as m]
            [malli.generator :as mg]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]))

(defspec test-non-empty-string-generator
         100
         (prop/for-all
           [s (mg/generator sut/non-empty-string)]
           (m/validate sut/non-empty-string s)))

(deftest test-non-empty-string
  (are [x y] (= x (m/validate sut/non-empty-string y))
             true "a"
             true " a"
             false ""
             false " "
             false "       "))

(defspec test-amount-generator
         100
         (prop/for-all
           [s (mg/generator sut/amount)]
           (m/validate sut/amount s)))

(deftest test-amount
  (are [x y] (= x (m/validate sut/amount y))
             true {:EUR 1000}
             false {}
             false {nil 1000}
             false {:EUR nil}
             false {"EUR" 1000}))

(defspec test-percentage-generator
         100
         (prop/for-all
           [s (mg/generator sut/percentage)]
           (m/validate sut/percentage s)))

(deftest test-percentage
  (are [x y] (= x (m/validate sut/percentage y))
             true 1
             true 1.0
             true 0.0
             true 0
             true 0.99
             true 1.1e-2

             false 1.01
             false -0.01
             false "asdf"
             false 99))

(defspec test-str-date-generator
         100
         (prop/for-all
           [s (mg/generator sut/str-date)]
           (m/validate sut/str-date s)))

(deftest test-str-date
  (are [x y] (= x (m/validate sut/str-date y))
             true "2020-12-13"
             false "2021-13-31"
             false "2021-12-32"
             false "2021-1-12"
             false "2021-01-1"
             false "221-01-12"
             false "asdfasdf"))

(deftest test-replace-merge
  (are [x y z] (= x (sut/replace-merge (m/schema [:map
                                                  [:a string?]
                                                  [:b string?]])
                                       y z))
               {} {:a "a" :b "b"} {}
               {:a "a" :b "b"} {} {:a "a" :b "b"}
               {:c "c"} {:a "a" :b "b" :c "c"} {}
               {:a "a1" :b "b1" :c "c1"} {:a "a" :b "b" :c "c"} {:a "a1" :b "b1" :c "c1"}))