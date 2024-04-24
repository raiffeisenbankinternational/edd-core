(ns edd.view-store.postgres.api-test
  (:require
   [clojure.test :refer [deftest is]]
   [edd.view-store.postgres.api :as api]))

(deftest test-db-schema
  (let [ctx
        {:service-name "foo-bar"
         :meta {:realm "aaa"}}]

    (is (= [[:. :aaa-foo-bar :aggregates]]
           (api/ctx->table ctx)))

    (try
      (api/ctx->table {:meta {:realm :test}})
      (is false)
      (catch Exception e
        (is (= "service-name is not set"
               (ex-message e)))))

    (try
      (api/ctx->table {:service-name "aa"})
      (is false)
      (catch Exception e
        (is (= "realm is not set"
               (ex-message e)))))))

(deftest test->fq-table
  (is (= [[:. :foo-bar-baz :aggregates]]
         (api/->table :foo-bar-baz))))
