(ns lambda.fx-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.el.cmd :as cmd]))

(use 'clojure.test)

(defn ctx
  []
  (-> {}
      (edd.core/reg-fx
       (fn [ctx events]
         {:service  "test-svc"
          :commands {:id     "1"
                     :cmd-id "2"}}))))

(deftest test-apply-fxwhen-fx-returns-map
  (let [resp (cmd/handle-effects (ctx) [])]
    (is (contains? (first resp) :service))))

(defn ctx2
  []
  (-> {}
      (edd.core/reg-fx
       (fn [ctx events]
         [{:service  "test-svc"
           :commands {:id     "1"
                      :cmd-id "2"}}]))))

(deftest test-apply-fxwhen-fx-returns-list
  (let [resp (cmd/handle-effects (ctx2) [])]
    (is (contains? (first resp) :service))))