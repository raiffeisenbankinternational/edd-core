(ns lambda.core-test
  (:require
   [aws.lambda :as core]
   [clojure.test :refer [deftest is]]
   [lambda.filters :as filters]
   [lambda.request :as request]))

(def ctx-filter
  {:init (fn [ctx]
           (assoc ctx :init-value "bla"))
   :cond (fn [{:keys [body]}]
           (contains? body :test))
   :fn   (fn [{:keys [_body] :as ctx}]
           (assoc ctx
                  :body {:resp "Bla"}))})

(def ctx {:filters [ctx-filter]})

(def user
  {:id "anon"
   :email "anon@ymous.com"
   :roles [:anonymous]
   :selected-role :anonymous})

(deftest test-apply-filter
  (let [resp (core/apply-filters
              (assoc ctx
                     :req {:test "Yes"}))]
    (is (=  {:resp "Bla"}
            (:body resp)))))

(deftest test-init-filter
  (let [ctx (core/init-filters {:some-value "true"
                                :filters [ctx-filter
                                          filters/to-api
                                          filters/from-api]})]
    (is (= "true"
           (:some-value ctx)))
    (is (= "bla"
           (:init-value ctx)))))

(deftest test-store-request-ids
  (binding [request/*request*
            (atom {:a "test"
                   :mdc {:foo "bar"
                         :invocation-id 999}})]
    (let [payload {:request-id 1
                   :invocation-id 2
                   :interaction-id 3
                   :test 42}]
      (filters/store-request-ids! payload))
    (is (= {:a "test"
            :mdc {:foo "bar"
                  :request-id 1
                  :invocation-id 2
                  :interaction-id 3}}
           @request/*request*))))
