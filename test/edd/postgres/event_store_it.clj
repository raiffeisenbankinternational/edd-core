(ns edd.postgres.event-store-it
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.state :refer [*dal-state*]]
            [edd.core :as edd]
            [edd.postgres.event-store :as dal]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock])
  (:import (org.postgresql.util PSQLException)))

(def fx-id (uuid/gen))

(defn get-ctx
  [invocation-id]
  (-> {}
      (assoc :service-name "local-test")
      (assoc :invocation-id invocation-id)
      (event-store/register)
      (view-store/register)
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            [{:identity (:id cmd)}
                             {:sequence (:id cmd)}
                             {:id       (:id cmd)
                              :event-id :event-1
                              :name     (:name cmd)}
                             {:id       (:id cmd)
                              :event-id :event-2
                              :name     (:name cmd)}]))
      (edd/reg-event :event-1
                     (fn [agg event]
                       (merge agg
                              {:value "1"})))
      (edd/reg-fx (fn [ctx events]
                    [{:commands [{:cmd-id :vmd-2
                                  :id     fx-id}]
                      :service  :s2}
                     {:commands [{:cmd-id :vmd-2
                                  :id     fx-id}]
                      :service  :s2}]))
      (edd/reg-event :event-2
                     (fn [agg event]
                       (merge agg
                              {:value "2"})))))

(deftest apply-when-two-events
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (get-ctx invocation-id)
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          req {:request-id     request-id
               :interaction-id interaction-id
               :commands       [{:cmd-id :cmd-1
                                 :id     agg-id}]}
          apply {:request-id     interaction-id
                 :interaction-id request-id
                 :apply          {:aggregate-id agg-id}}
          resp (edd/handler ctx
                            (assoc req :log-level :debug))]
      (edd/handler ctx apply)
      (mock/verify-state :aggregate-store
                         [{:id      agg-id
                           :version 2
                           :value   "2"}])
      (is (= {:result         {:effects    [{:cmd-id       :vmd-2
                                             :id           fx-id
                                             :service-name :s2}
                                            {:cmd-id       :vmd-2
                                             :id           fx-id
                                             :service-name :s2}]
                               :events     2
                               :identities 1
                               :meta       [{:cmd-1 {:id agg-id}}]
                               :sequences  1
                               :success    true}
              :interaction-id interaction-id
              :request-id     request-id}
             resp))
      (is (contains? (edd/handler ctx
                                  (assoc req :log-level :debug))
                     :error)))))