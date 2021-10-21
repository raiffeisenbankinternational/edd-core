(ns edd.schema.core-test
  (:require [edd.schema.core :as sut]
            [edd.schema.swagger :as swagger]
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
            [jsonista.core :as json]
            [yaml.core :as yaml]))

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

(deftest swagger-schema-test
  (mock/with-mock-dal
    (let [ctx (-> mock/ctx
                  (edd/reg-cmd :dummy-cmd (fn [ctx cmd]
                                            [])
                               :spec [:map
                                      [:date [:re {:error/message      "Not a valid date. The format should be YYYY-MM-DD"
                                                   :json-schema/type   "string"
                                                   :json-schema/format "date"}
                                              #"\d{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])"]]
                                      [:name string?]])
                  (edd/reg-query :query-1 (fn [ctx query]
                                            [])
                                 :consumes [:map]
                                 :produces [:map]))]
      (is (= {"components" {"schemas" {"command-error"    {"properties" {"errors"         {"items" {"properties" {}
                                                                                                    "type"       "object"}
                                                                                           "type"  "array"}
                                                                         "interaction-id" {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "invocation-id"  {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "request-id"     {"format" "uuid"
                                                                                           "type"   "string"}}
                                                           "required"   ["errors"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "command-success"  {"properties" {"interaction-id" {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "invocation-id"  {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "request-id"     {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "result"         {"properties" {"effects"    {"description" "Indicates how many asnc actions where triggers"
                                                                                                                       "format"      "int64"
                                                                                                                       "type"        "integer"}
                                                                                                         "events"     {"description" "NUmber of events produced"
                                                                                                                       "format"      "int64"
                                                                                                                       "type"        "integer"}
                                                                                                         "identities" {"description" "Number of identities created"
                                                                                                                       "format"      "int64"
                                                                                                                       "type"        "integer"}
                                                                                                         "sequences"  {"description" "Number of sequences created"
                                                                                                                       "format"      "int64"
                                                                                                                       "type"        "integer"}
                                                                                                         "success"    {"description" "Indicates if response was successfull"
                                                                                                                       "type"        "boolean"}}
                                                                                           "required"   ["success"
                                                                                                         "effects"
                                                                                                         "events"
                                                                                                         "identities"
                                                                                                         "sequences"]
                                                                                           "type"       "object"}}
                                                           "required"   ["result"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "dummy-cmd"        {"properties" {"command"        {"properties" {"cmd-id" {"enum" [":dummy-cmd"]
                                                                                                                   "type" "string"}
                                                                                                         "date"   {"format"  "date"
                                                                                                                   "pattern" "\\d{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])"
                                                                                                                   "type"    "string"}
                                                                                                         "id"     {"format" "uuid"
                                                                                                                   "type"   "string"}
                                                                                                         "name"   {"type" "string"}}
                                                                                           "required"   ["id"
                                                                                                         "cmd-id"
                                                                                                         "date"
                                                                                                         "name"]
                                                                                           "type"       "object"}
                                                                         "interaction-id" {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "request-id"     {"format" "uuid"
                                                                                           "type"   "string"}}
                                                           "required"   ["request-id"
                                                                         "interaction-id"
                                                                         "command"]
                                                           "type"       "object"}
                                       "query-1-consumes" {"properties" {"interaction-id" {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "query-id"       {"enum" [":query-1"]
                                                                                           "type" "string"}
                                                                         "request-id"     {"format" "uuid"
                                                                                           "type"   "string"}}
                                                           "required"   ["query-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "query-1-produces" {"properties" {"interaction-id" {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "invocation-id"  {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "request-id"     {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "result"         {"properties" {}
                                                                                           "type"       "object"}}
                                                           "required"   ["result"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "query-error"      {"properties" {"errors"         {"items" {"properties" {}
                                                                                                    "type"       "object"}
                                                                                           "type"  "array"}
                                                                         "interaction-id" {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "invocation-id"  {"format" "uuid"
                                                                                           "type"   "string"}
                                                                         "request-id"     {"format" "uuid"
                                                                                           "type"   "string"}}
                                                           "required"   ["errors"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}}}
              "info"       {"title"   "api"
                            "version" "1.0"}
              "openapi"    "3.0.3"
              "paths"      {"/command/dummy-cmd" {"post" {"description" ""
                                                          "requestBody" {"content"  {"application/json" {"schema" {"$ref" "#/components/schemas/dummy-cmd"}}}
                                                                         "required" true}
                                                          "responses"   {"200" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/command-success"}}}
                                                                                "description" "OK"}
                                                                         "501" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/command-error"}}}
                                                                                "description" "OK"}}
                                                          "summary"     ""}}
                            "/query/query-1"     {"post" {"description" ""
                                                          "requestBody" {"content"  {"application/json" {"schema" {"$ref" "#/components/schemas/query-1-consumes"}}}
                                                                         "required" true}
                                                          "responses"   {"200" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/query-1-produces"}}}
                                                                                "description" "OK"}
                                                                         "501" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/query-error"}}}
                                                                                "description" "OK"}}
                                                          "summary"     ""}}}}
             (json/read-value
               (with-out-str
                 (lambda-core/start
                   (assoc ctx :edd/runtime "edd.schema.swagger/swagger-runtime"
                              :edd/schema-format "json")))))))))