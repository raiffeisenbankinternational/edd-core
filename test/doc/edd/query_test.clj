(ns doc.edd.query-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
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

  (testing "It is possible to register custom schemas using :consumes
            and :produces keywords"
    (let [response (atom {:id 1})
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
                                                      @response)
                                 :consumes [:map
                                            [:first-name :string]]
                                 :produces [:map
                                            [:id :int]]))]

      (testing "Invalid request"
        (is (thrown? ExceptionInfo
                     (edd-query/handle-query ctx invalid-request)))
        (try
          (edd-query/handle-query ctx invalid-request)
          (catch ExceptionInfo ex
            (is (= {:error {:first-name ["missing required key"]}}
                   (ex-data ex))))))

      (testing "Valid request, valid response"
        (is (= @response
               (edd-query/handle-query ctx request))))

      (testing "Valid request, invalid response"
        (reset! response {:id "5"})

        (testing "No error when response-schema-validation is unset"
          (is (= @response
                 (edd-query/handle-query ctx request))))

        (testing "Exception when response-schema-validation is set on context"
          (let [query-fn #(-> ctx
                              (assoc :response-schema-validation :throw-on-error)
                              (edd-query/handle-query request))]
            (is (thrown? ExceptionInfo (query-fn)))
            (try
              (query-fn)
              (catch ExceptionInfo ex
                (is (= {:error    {:result {:id ["should be an integer"]}}
                        :response {:result {:id "5"}}}
                       (ex-data ex)))))))

        (testing "Exception when response-schema-validation is set in meta"
          (let [query-fn #(-> ctx
                              (assoc-in [:meta :response-schema-validation] :throw-on-error)
                              (edd-query/handle-query request))]
            (is (thrown? ExceptionInfo (query-fn)))
            (try
              (query-fn)
              (catch ExceptionInfo ex
                (is (= {:error    {:result {:id ["should be an integer"]}}
                        :response {:result {:id "5"}}}
                       (ex-data ex)))))))

        (testing "Log warning when response-schema-validation is set on context"
          (let [warning (atom nil)]
            (with-redefs [log/log* (fn [_ level _ message]
                                     (when (= :warn level)
                                       (reset! warning message)))]
              (-> ctx
                  (assoc :response-schema-validation :log-on-error)
                  (edd-query/handle-query request)))
            (is (re-matches #".*should be an integer.*" @warning))))

        (testing "Log warning when response-schema-validation is set on meta"
          (let [warning (atom nil)]
            (with-redefs [log/log* (fn [_ level _ message]
                                     (when (= :warn level)
                                       (reset! warning message)))]
              (-> ctx
                  (assoc-in [:meta :response-schema-validation] :log-on-error)
                  (edd-query/handle-query request)))
            (is (re-matches #".*should be an integer.*" @warning)))))

      (let [helper (fn [produces response]
                     (try
                       (-> init-ctx
                           (assoc :response-schema-validation :throw-on-error)
                           (edd/reg-query :test-query (constantly response)
                                          :produces produces)
                           (edd-query/handle-query {:query {:query-id :test-query}}))
                       :no-error
                       (catch ExceptionInfo _e
                         :error)))]

        (testing "Allows any return value in case query doesn't have explicit :produces"
          (are
           [produces response error-status] (= error-status (helper produces response))
            nil      nil      :no-error
            nil      5        :no-error
            nil      [1 2]    :no-error
            nil      #{1 2}   :no-error
            nil      {:a :b}  :no-error
            nil      false    :no-error))

        (testing "Validates non-map return values correctly"
          (are
           [produces response error-status] (= error-status (helper produces response))
            :any               nil     :no-error
            :any               5       :no-error
            :any               [1 2]   :no-error
            :any               #{1 2}  :no-error
            :any               {:a :b} :no-error
            :any               false   :no-error
            :boolean           nil     :error
            :boolean           true    :no-error
            [:sequential :int] [1 2]   :no-error
            [:sequential :map] [1 2]   :error))))))
