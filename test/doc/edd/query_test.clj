(ns doc.edd.query-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.el.query :as edd-query]
            [doc.edd.ctx :refer [init-ctx]])
  (:import (clojure.lang ExceptionInfo)))

(deftest register-query-test
  "# To register query you need invoke edd.core/reg-query function"
  (-> init-ctx
      (edd/reg-query :get-by-id (fn [ctx query]
                                  (comment "Here we can put handler for query.
                                            It can return map or vector with response"))))
  (testing "Lets register query that returns fixed response and then execute it.
            To execute query we will need to create query. Same structure of
            query you would send from API to the handler"
    (let [request {:query {:query-id :get-by-id
                           :id       "user-id-1"}}
          ctx (-> init-ctx
                  (edd/reg-query :get-by-id (fn [ctx query]
                                              ; We just check if they match
                                              (is (= query
                                                     (:query request)))
                                              ; We return some value as response
                                              {:first-name "John"})
                                 :produces [:map]))
          ; We can now handle request with new context containing information
          ; about registered query handler. This method will bot be used explicitly
          ; in your code, but can be user for writing tests
          response (edd-query/handle-query ctx request)]
      (is (= {:first-name "John"}
             response))))

  (testing "When unknown query is triggered ex-info is raised"
    (let [request {:query {:query-id :get-by-unknown-handler
                           :id       "user-id-1"}}
          ctx (-> init-ctx
                  (edd/reg-query :get-by-id (fn [ctx query]
                                              ; We just check if they match
                                              (is (= query
                                                     (:query request)))
                                              ; We return some value as response
                                              {:first-name "John"})))]
      (is (thrown? ExceptionInfo
                   (edd-query/handle-query ctx request)))))

  (testing "When schema validation fails ex-info is raised"
    (let [request-missing-query-id {:query {:id "Query with missing event-id"}}
          request-unknown-query-id {:query {:id       "Non existing query-id"
                                            :query-id :some-random-query}}
          ctx (-> init-ctx
                  (edd/reg-query :get-by-id (fn [ctx query]
                                              {:first-name "John"})
                                 :produces [:map]))]
      (is (thrown? ExceptionInfo
                   (edd-query/handle-query ctx request-missing-query-id)))
      (try
        (edd-query/handle-query ctx request-missing-query-id)
        (catch ExceptionInfo ex
          (is (= {:error    "No handler found"
                  :query-id nil}
                 (ex-data ex)))))

      (is (thrown? ExceptionInfo
                   (edd-query/handle-query ctx request-unknown-query-id)))
      (try
        (edd-query/handle-query ctx request-unknown-query-id)
        (catch ExceptionInfo ex
          (is (= {:error    "No handler found"
                  :query-id :some-random-query}
                 (ex-data ex)))))))

  (testing "It is possible to register custom schema
            using :consumes keyword. Invalid request
            also reaises ex-info"
    (let [response {:first-name "John"}
          request {:query {:query-id   :get-by-first-name
                           :first-name "John"}}
          invalid-request {:query {:query-id  :get-by-first-name
                                   :last-name "Last name instead of first-name"}}
          ctx (-> init-ctx
                  (edd/reg-query :get-by-first-name (fn [ctx query]
                                                      ; We just check if they match
                                                      (is (= query
                                                             (:query request)))
                                                      ; We return some value as response
                                                      response)
                                 :consumes [:map
                                            [:first-name :string]]))]
      (is (= response
             (edd-query/handle-query ctx request)))
      (is (thrown? ExceptionInfo
                   (edd-query/handle-query ctx invalid-request)))
      (try
        (edd-query/handle-query ctx invalid-request)
        (catch ExceptionInfo ex
          (is (= {:error {:first-name ["missing required key"]}}
                 (ex-data ex))))))))