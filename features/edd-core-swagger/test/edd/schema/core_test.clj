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
                                                                                                    "required"   []
                                                                                                    "type"       "object"}
                                                                                           "type"  "array"}
                                                                         "interaction-id" {"description" "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "invocation-id"  {"description" "Invocation ID represents backend invocation
                                  id for this execution."
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "request-id"     {"description" "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}}
                                                           "required"   ["errors"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "command-success"  {"properties" {"interaction-id" {"description" "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "invocation-id"  {"description" "Invocation ID represents backend invocation
                                  id for this execution."
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "request-id"     {"description" "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
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
                                                                         "interaction-id" {"description" "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "request-id"     {"description" "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}}
                                                           "required"   ["request-id"
                                                                         "interaction-id"
                                                                         "command"]
                                                           "type"       "object"}
                                       "query-1-consumes" {"properties" {"interaction-id" {"description" "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "query-id"       {"enum" [":query-1"]
                                                                                           "type" "string"}
                                                                         "request-id"     {"description" "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}}
                                                           "required"   ["query-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "query-1-produces" {"properties" {"interaction-id" {"description" "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "invocation-id"  {"description" "Invocation ID represents backend invocation
                                  id for this execution."
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "request-id"     {"description" "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "result"         {"properties" {}
                                                                                           "required"   []
                                                                                           "type"       "object"}}
                                                           "required"   ["result"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}
                                       "query-error"      {"properties" {"errors"         {"items" {"properties" {}
                                                                                                    "required"   []
                                                                                                    "type"       "object"}
                                                                                           "type"  "array"}
                                                                         "interaction-id" {"description" "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "invocation-id"  {"description" "Invocation ID represents backend invocation
                                  id for this execution."
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}
                                                                         "request-id"     {"description" "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request"
                                                                                           "format"      "uuid"
                                                                                           "type"        "string"}}
                                                           "required"   ["errors"
                                                                         "invocation-id"
                                                                         "request-id"
                                                                         "interaction-id"]
                                                           "type"       "object"}}}
              "info"       {"description" "api"
                            "title"       "api"
                            "version"     "1.0"}
              "openapi"    "3.0.3"
              "paths"      {"/command/dummy-cmd" {"post" {"description" ""
                                                          "requestBody" {"content"  {"application/json" {"schema" {"$ref" "#/components/schemas/dummy-cmd"}}}
                                                                         "required" true}
                                                          "responses"   {"200" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/command-success"}}}
                                                                                "description" "OK"}
                                                                         "501" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/command-error"}}}
                                                                                "description" "OK"}}
                                                          "summary"     ""}}
                            "/query/query-1"     {"get" {"description" ""
                                                         "parameters"  [{"in"       "query"
                                                                         "name"     "query"
                                                                         "required" true
                                                                         "schema"   {"$ref" "#/components/schemas/query-1-consumes"}}]
                                                         "responses"   {"200" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/query-1-produces"}}}
                                                                               "description" "OK"}
                                                                        "404" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/query-error"}}}
                                                                               "description" "Not found"}
                                                                        "501" {"content"     {"application/json" {"schema" {"$ref" "#/components/schemas/query-error"}}}
                                                                               "description" "Internal error"}}
                                                         "summary"     ""}}}}
             (json/read-value
               (with-out-str
                 (lambda-core/start
                   (assoc ctx :edd/runtime "edd.schema.swagger/swagger-runtime"
                              :edd/schema-format "json")))))))))