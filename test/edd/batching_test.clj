(ns edd.batching-test
  (:require
   [edd.test.fixture.execution :as exec]
   [edd.core :as edd]
   [edd.common :as common]
   [lambda.uuid :as uuid]
   [edd.test.fixture.dal :as mock]
   [edd.elastic.view-store :as view-store]
   [lambda.test.fixture.client :as client]
   [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]
   [lambda.request :as request]))

(defn counter-ctx []
  (-> mock/ctx
      (assoc :service-name :local-test)
      (edd/reg-cmd :inc
                   (fn [ctx cmd]
                     {:event-id :inced
                      :value    (inc (get-in ctx [:counter :value] 0))})
                   :dps {:counter
                         (fn [cmd]
                           {:query-id :get-by-id
                            :id       (:id cmd)})})
      (edd/reg-cmd :inc-2
                   (fn [ctx cmd]
                     (if (> 0 (get @request/*request* :attempt 0))
                       {:event-id :inced
                        :value    (inc (get-in ctx [:counter :value] 0))}
                       (do
                         (swap! request/*request* #(assoc % :attempt 0))
                         (throw (ex-info "Fail" {:on :purpose})))))
                   :dps {:counter
                         (fn [cmd]
                           {:query-id :get-by-id
                            :id       (:id cmd)})})

      (edd/reg-query :get-by-id common/get-by-id)
      (edd/reg-event :inced (fn [ctx event]
                              {:value (:value event)}))))

(def id1 (uuid/parse "111111-1111-1111-1111-111111111111"))
(def id2 (uuid/parse "222222-2222-2222-2222-222222222222"))

(deftest test-batched-values-should-sum-up-correctly
  (let [ctx (counter-ctx)]
    (mock/with-mock-dal
      (exec/run-cmd! ctx {:commands [{:cmd-id :inc
                                      :id     id1}
                                     {:cmd-id :inc
                                      :id     id1}]})
      (mock/verify-state :aggregate-store
                         [{:id      id1
                           :value   2
                           :version 2}]))))

(deftest test-two-commands-non-batched-should-sum-up-correctly
  (let [ctx (counter-ctx)]
    (mock/with-mock-dal
      (exec/run-cmd! ctx {:commands [{:cmd-id :inc
                                      :id     id1}]})

      (mock/verify-state :aggregate-store
                         [{:id      id1
                           :value   1
                           :version 1}])

      (exec/run-cmd! ctx {:commands [{:cmd-id :inc
                                      :id     id1}]})

      (mock/verify-state :aggregate-store
                         [{:id      id1
                           :value   2
                           :version 2}]))))

(deftest test-batched-values-for-2-counters-should-not-interfer
  (let [ctx (counter-ctx)]
    (mock/with-mock-dal
      (exec/run-cmd! ctx {:commands [{:cmd-id :inc
                                      :id     id1}
                                     {:cmd-id :inc
                                      :id     id2}
                                     {:cmd-id :inc
                                      :id     id1}
                                     {:cmd-id :inc
                                      :id     id2}]})

      (mock/verify-state :aggregate-store
                         [{:id      id1
                           :value   2
                           :version 2}
                          {:id      id2
                           :value   2
                           :version 2}]))))

(deftest test-batched-values-for-2-counters-should-not-interfer
  (let [first-id (uuid/gen)
        second-id (uuid/gen)
        ctx (-> mock/ctx
                (edd/reg-cmd :first-one (fn [_ctx cmd]
                                          [{:event-id :first-event}
                                           {:identity (:name cmd)}]))
                (edd/reg-cmd :second-one (fn [{:keys [first-one
                                                      first-one-id]} cmd]
                                           (is (= first-id
                                                  first-one))
                                           (is (= {:first true,
                                                   :version 1,
                                                   :id first-id}
                                                  first-one-id))
                                           [{:identity (:name cmd)}])
                             :deps {:first-one (fn [_ctx cmd]
                                                 {:query-id :identity-query
                                                  :name (:first-name cmd)})
                                    :first-one-id (fn [_ctx cmd]
                                                    {:query-id :id-query
                                                     :id (:cmd-first-id cmd)})})
                (edd/reg-query :identity-query (fn [ctx query]
                                                 (common/get-aggregate-id-by-identity
                                                  ctx
                                                  (:name query))))
                (edd/reg-event :first-event (fn [agg event]
                                              (merge agg
                                                     {:first true})))
                (edd/reg-query :id-query (fn [ctx query]
                                           (common/get-by-id
                                            ctx
                                            query))))]
    (mock/with-mock-dal
      (mock/handle-cmd ctx {:commands [{:cmd-id :first-one
                                        :id     first-id
                                        :name "first"}
                                       {:cmd-id :second-one
                                        :id     second-id
                                        :cmd-first-id first-id
                                        :first-name "first"
                                        :name "second"}]})

      (mock/verify-state :identity-store
                         [{:id first-id
                           :identity "first"}
                          {:id second-id
                           :identity "second"}]))))



