(ns edd.fx-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.el.cmd :as cmd]
            [edd.memory.view-store :as view-store]
            [edd.memory.event-store :as event-store]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]))

(use 'clojure.test)

(deftest test-meta-fx
  (let [request-id (uuid/gen)
        interaction-id (uuid/gen)
        ctx (-> {:request-id     request-id
                 :interaction-id interaction-id}
                (edd.core/reg-fx
                 (fn [ctx events]
                   [{:service  "test-svc"
                     :commands {:id     "1"
                                :cmd-id "2"}}
                    {:service  "test-svc-2"
                     :commands {:id     "3"
                                :cmd-id "4"}}])))
        resp (cmd/handle-effects ctx)
        resp (get-in resp [:resp :commands])]
    (is (= [{:commands       {:cmd-id "2"
                              :id     "1"}
             :interaction-id interaction-id
             :meta           {}
             :request-id     request-id
             :service        "test-svc"}
            {:commands       {:cmd-id "4"
                              :id     "3"}
             :interaction-id interaction-id
             :meta           {}
             :request-id     request-id
             :service        "test-svc-2"}]
           resp))))

(def ctx
  (-> {}
      (edd.core/reg-fx
       (fn [ctx events]
         {:service  "test-svc"
          :commands {:id     "1"
                     :cmd-id "2"}}))))

(deftest test-apply-fx-when-fx-returns-map
  (let [resp (cmd/handle-effects ctx)
        resp (get-in resp [:resp :commands])]
    (is (contains? (first resp) :service))))

(def ctx2
  (-> {}
      (edd.core/reg-fx
       (fn [ctx events]
         [{:service  "test-svc"
           :commands {:id     "1"
                      :cmd-id "2"}}]))))

(deftest test-apply-fx-when-fx-returns-list
  (let [resp (cmd/handle-effects ctx2)
        resp (get-in resp [:resp :commands])]
    (is (contains? (first resp) :service))))

(def cmd-id #uuid "22222111-1111-1111-1111-111111111111")
(def fx-id #uuid "22222111-1111-1111-1111-111111111111")

(def ctx3
  (-> mock/ctx
      (merge {:service-name "source-svc"})
      (edd.core/reg-cmd
       :cmd-1
       (fn [ctx cmd]
         {:event-id :e1}))
      (edd.core/reg-fx
       (fn [ctx events]
         [{:id     "2"
           :cmd-id "2"}
          {:service  "target-svc"
           :commands [{:id     "1"
                       :cmd-id "1"}]}]))))

(deftest test-command-storage
  (mock/with-mock-dal
    (mock/handle-cmd ctx3 {:id     cmd-id
                           :cmd-id :cmd-1})
    (mock/verify-state :command-store
                       [{:service  "source-svc"
                         :commands [{:id     "2"
                                     :cmd-id "2"}]
                         :meta {}}
                        {:service  "target-svc"
                         :commands [{:id     "1"
                                     :cmd-id "1"}]
                         :meta {}}])))