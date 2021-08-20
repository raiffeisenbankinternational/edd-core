(ns edd.postgres.event-store-it
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.state :refer [*dal-state*]]
            [edd.core :as edd]
            [edd.postgres.event-store :as dal]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [lambda.util :as util])
  (:import (org.postgresql.util PSQLException)))

(def fx-id (uuid/gen))

(defn get-ctx
  [invocation-id]
  (-> {}
      (assoc :service-name "local-test")
      (assoc :invocation-id invocation-id)
      (assoc :environment-name-lower (util/get-env "EnvironmentNameLower"))
      (assoc :aws {:account-id (util/get-env "AccountId")})
      (assoc :meta {:realm :test})
      (event-store/register)
      (view-store/register)
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            [{:identity (:id cmd)}
                             {:sequence (:id cmd)}
                             {:id (:id cmd)
                              :event-id :event-1
                              :name (:name cmd)}
                             {:id (:id cmd)
                              :event-id :event-2
                              :name (:name cmd)}]))
      (edd/reg-event :event-1
                     (fn [agg event]
                       (merge agg
                              {:value "1"})))
      (edd/reg-fx (fn [ctx events]
                    [{:commands [{:cmd-id :vmd-2
                                  :id fx-id}]
                      :service :s2}
                     {:commands [{:cmd-id :vmd-2
                                  :id fx-id}]
                      :service :s2}]))
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
          req {:request-id request-id
               :interaction-id interaction-id
               :commands [{:cmd-id :cmd-1
                           :id agg-id}]}
          apply {:request-id interaction-id
                 :interaction-id request-id
                 :meta {:realm :test}
                 :apply {:aggregate-id agg-id}}
          resp (edd/handler ctx
                            (assoc req :log-level :debug))]
      (edd/handler ctx apply)
      (mock/verify-state :aggregate-store
                         [{:id agg-id
                           :version 2
                           :value "2"}])
      (is (= {:result {:effects [{:cmd-id :vmd-2
                                  :id fx-id
                                  :service-name :s2}
                                 {:cmd-id :vmd-2
                                  :id fx-id
                                  :service-name :s2}]
                       :events 2
                       :identities 1
                       :meta [{:cmd-1 {:id agg-id}}]
                       :sequences 1
                       :success true}
              :invocation-id invocation-id
              :interaction-id interaction-id
              :request-id request-id}
             resp)))))

(deftest test-transaction-when-saving
  (with-redefs [event-store/store-cmd (fn [ctx cmd]
                                        (throw (ex-info "CMD Store failure" {})))]
    (binding [*dal-state* (atom {})]
      (let [invocation-id (uuid/gen)
            ctx (get-ctx invocation-id)
            agg-id (uuid/gen)
            interaction-id (uuid/gen)
            request-id (uuid/gen)
            req {:request-id request-id
                 :interaction-id interaction-id
                 :commands [{:cmd-id :cmd-1
                             :id agg-id}]}
            resp (edd/handler ctx
                              (assoc req :log-level :debug))]

        (edd/with-stores
          ctx (fn [ctx]
                (is (= []
                       (event-store/get-response-log ctx invocation-id)))))

        (is (= {:error "CMD Store failure"
                :invocation-id invocation-id
                :interaction-id interaction-id
                :request-id request-id}
               resp))))))

(deftest test-sequence-when-saving-error
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (-> (get-ctx invocation-id)
                  (edd/reg-cmd :cmd-i1 (fn [ctx cmd]
                                         [{:sequence (:id cmd)}
                                          {:event-id :i1-created}]))
                  (edd/reg-cmd :cmd-i2 (fn [ctx cmd]
                                         [{:event-id :i1-created}])
                               :dps {:seq edd.common/get-sequence-number-for-id})
                  (edd/reg-fx (fn [ctx events]
                                {:commands [{:cmd-id :cmd-i2
                                             :id (:id (first events))}]
                                 :service :s2})))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          agg-id-2 (uuid/gen)
          req {:request-id request-id
               :interaction-id interaction-id
               :commands [{:cmd-id :cmd-i1
                           :id agg-id}]}
          resp (with-redefs [event-store/update-sequence (fn [ctx cmd]
                                                           (throw (ex-info "Sequence Store failure" {})))]
                 (edd/handler ctx
                              (assoc req :log-level :debug)))]

      (edd/handler ctx
                   {:request-id (uuid/gen)
                    :interaction-id interaction-id
                    :commands [{:cmd-id :cmd-i1
                                :id agg-id-2}]})
      (edd/with-stores
        ctx (fn [ctx]
              (is (not= []
                        (event-store/get-response-log ctx invocation-id)))
              (let [seq (event-store/get-sequence-number-for-id-imp
                         (assoc ctx :id agg-id))]
                (is (> seq
                       0))
                (is (= (dec seq)
                       (event-store/get-sequence-number-for-id-imp
                        (assoc ctx :id agg-id-2)))))
              (is (= nil
                     (event-store/get-sequence-number-for-id-imp
                      (assoc ctx :id (uuid/gen)))))))

      (is (= {:error "Sequence Store failure"
              :invocation-id invocation-id
              :interaction-id interaction-id
              :request-id request-id}
             resp)))))

(deftest test-saving-of-request-error
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (get-ctx invocation-id)
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          req {:request-id request-id
               :interaction-id interaction-id
               :commands [{:cmd-id :cmd-1
                           :no-id "schema should fail"}]}
          resp (edd/handler ctx
                            (assoc req :log-level :debug))
          expected-error {:spec [{:id ["missing required key"]}]}]

      (edd/with-stores
        ctx (fn [ctx]
              (is (= [{:error expected-error}]
                     (mapv :error (event-store/get-request-log ctx request-id ""))))))

      (is (= {:error expected-error
              :invocation-id invocation-id
              :interaction-id interaction-id
              :request-id request-id}
             resp)))))
