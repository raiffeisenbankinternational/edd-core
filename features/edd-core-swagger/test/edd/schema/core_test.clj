(ns edd.schema.core-test
  (:require [edd.schema.core :as sut]
            [clojure.test.check.properties :as prop]
            [edd.test.fixture.dal :as mock]
            [malli.core :as m]
            [malli.generator :as mg]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]
            [edd.core :as edd]
            [lambda.core :as lambda-core]
            [jsonista.core :as json]))


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