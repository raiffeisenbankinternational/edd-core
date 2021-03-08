(ns edd.batching-test
  (:require
   [edd.test.fixture.execution :as exec]
   [edd.core :as edd]
   [edd.common :as common]
   [lambda.uuid :as uuid]
   [edd.test.fixture.dal :as mock]
   [clojure.test :refer [deftest testing is are use-fixtures run-tests join-fixtures]]))

(defn counter-ctx []
  (-> mock/ctx
      (assoc :service-name :local-test)
      (edd/reg-cmd :inc
                   (fn [ctx cmd]
                     {:event-id :inced
                      :value     (inc (get-in ctx [:counter :value] 0))})
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
