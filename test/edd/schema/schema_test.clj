(ns edd.schema.schema-test
  (:require [edd.schema.core :as sut]
            [clojure.test.check.properties :as prop]
            [edd.test.fixture.dal :as mock]
            [malli.core :as m]
            [malli.generator :as mg]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]
            [edd.core :as edd]
            [lambda.core :as lambda-core]
            [jsonista.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

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
      (is (= (edn/read-string (slurp (io/resource "schema.edn")))
             (json/read-value
              (with-out-str
                (lambda-core/start
                 (assoc ctx :edd/runtime "edd.schema.swagger/swagger-runtime"
                        :edd/schema-format "json")))))))))