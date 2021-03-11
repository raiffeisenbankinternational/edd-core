(ns edd.snapshot-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [edd.el.event :as event]

            [edd.core :as edd]
            [edd.test.fixture.dal :as mock]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [edd.dal :as dal]))

(def agg-id (uuid/gen))

(def ctx
  (-> {}
      (assoc :service-name "local-test")
      (event-store/register)
      (view-store/register)

      (edd/reg-event :event-1
                     (fn [agg event]
                       (update agg :value (fnil inc 0))))))

(deftest apply-events-no-snapshot
  (testing "snapshot - but no snapshot available"
    (mock/with-mock-dal
      {:event-store [{:event-id :event-1
                      :event-seq 1
                      :id       agg-id}]}
      (let [resp (event/handle-event (assoc-in ctx [:apply :aggregate-id] agg-id))]
        (mock/verify-state :aggregate-store
                           [{:id    agg-id
                             :version 1
                             :value 1}])))))

(deftest apply-events-snapshot-and-no-events
  (testing "snapshot available and events empty"
    (mock/with-mock-dal
      {:aggregate-store [{:id      agg-id
                          :value   2
                          :version 2}]}
      (let [resp (event/handle-event (assoc-in ctx [:apply :aggregate-id] agg-id))]
        (prn resp)
        (mock/verify-state :aggregate-store
                           [{:id      agg-id
                             :version 2
                             :value   2}])))))

(deftest apply-events-snapshot-and-newer-events
  (testing "snapshot available and one newer event"
    (mock/with-mock-dal
      {:event-store     [{:event-id  :event-1
                          :event-seq 3
                          :id        agg-id}]
       :aggregate-store [{:id      agg-id
                          :value   1
                          :version 2}]}
      (let [resp (event/handle-event (assoc-in ctx [:apply :aggregate-id] agg-id))]
        (mock/verify-state :aggregate-store
                           [{:id      agg-id
                             :version 3
                             :value   2}])))))

(deftest apply-events-snapshot-and-older-events
  (testing "snapshot available and older events, only snapshot will be considered"
    (mock/with-mock-dal
      {:event-store     [{:event-id :event-1
                          :event-seq 3
                          :id agg-id}]
       :aggregate-store [{:id      agg-id
                          :value   1
                          :version 3}]}
      (let [resp (event/handle-event (assoc-in ctx [:apply :aggregate-id] agg-id))]
        (mock/verify-state :aggregate-store
                           [{:id      agg-id
                             :version 3
                             :value   1}])))))
