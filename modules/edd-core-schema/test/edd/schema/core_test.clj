(ns edd.schema.core-test
  (:require [glms-schema.core :as sut]
            [glms-schema.swagger :as swagger]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [edd.test.fixture.dal :as mock]
            [malli.core :as m]
            [malli.generator :as mg]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]
            [edd.core :as edd]
            [lambda.core :as lambda-core]
            [lambda.util :as util]
            [jsonista.core :as json]))

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

(deftest test-missing-failed-custom-validation-command
  (mock/with-mock-dal
    (let [ctx (-> mock/ctx
                  (edd/reg-cmd :dummy-cmd (fn [ctx cmd]
                                            [])
                               :spec [:map
                                      [:name string?]])
                  (edd/reg-query :query-1 (fn [ctx query]
                                            [])
                                 ))]
      (is (= {"basePath"   "plc2-svc"
              "components" {"schemas" {"dummy-cmd" {"properties" {"cmd-id" {"type" "string"}
                                                                  "id"     {"format" "uuid"
                                                                            "type"   "string"}
                                                                  "name"   {"type" "string"}}
                                                    "required"   ["cmd-id"
                                                                  "id"
                                                                  "name"]
                                                    "type"       "object"}}}
              "host"       "plc2-svc.lime.internal.rbigroup.cloud"
              "info"       {"description" nil
                            "title"       nil
                            "version"     nil}
              "openapi"    "3.0.3"
              "paths"      {"/command" {"post" {"consumes"    ["application/json"]
                                                "description" ""
                                                "parameters"  [{"in"       "body"
                                                                "name"     "command"
                                                                "required" true
                                                                "schema"   {"oneOf" [{"$ref" "#/components/schemas/dummy-cmd"}]}}]
                                                "produces"    ["application/json"]
                                                "responses"   {"200" {"description" "OK"
                                                                      "type"        "object"}}
                                                "summary"     ""}}
                            "/query"   {"post" {"consumes"    ["application/json"]
                                                "description" ""
                                                "parameters"  [{"in"       "body"
                                                                "name"     "query"
                                                                "required" true
                                                                "schema"   {"oneOf" []}}]
                                                "produces"    ["application/json"]
                                                "responses"   {"200" {"description" "OK"
                                                                      "type"        "object"}}
                                                "summary"     ""}}}
              "schemes"    ["http"
                            "https"]}
             (json/read-value
               (with-out-str
                 (lambda-core/start
                   (assoc ctx :edd/runtime "glms-schema.swagger/swagger-runtime")))))))))