(ns lambda.core-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [edd.el.event :as event]
            [aws.lambda :as core]
            [lambda.filters :as filters]))

(def ctx-filter
  {:init (fn [ctx]
           (assoc ctx :init-value "bla"))
   :cond (fn [{:keys [body]}]
           (contains? body :test))
   :fn   (fn [{:keys [body] :as ctx}]
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