(ns edd.el.version-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.common :as common]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [edd.dal :as dal]))

(def ctx
  (-> mock/ctx
      (assoc :service-name :local-test)
      (edd/reg-cmd :cmd-1
                   (fn [ctx cmd]
                     {:event-id :event-1
                      :name     "Test name"})
                   :dps {:test-dps
                         (fn [cmd]
                           {:query-id :get-by-id
                            :id       (:id cmd)})})

      (edd/reg-query :get-by-id common/get-by-id)
      (edd/reg-event :event-1 (fn [ctx event]
                                {:name (:name event)}))))

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))

(require '[edd.test.fixture.execution :as exec])
(require '[lambda.test.fixture.state
           :as state])

@state/*dal-state*

(deftest test-version-with-no-aggregate
  (mock/with-mock-dal
    (mock/apply-cmd ctx {:cmd-id :cmd-1
                         :id     cmd-id})

    (mock/verify-state :event-store [{:event-id  :event-1
                                      :id        cmd-id
                                      :event-seq 1
                                      :meta      {}
                                      :name      "Test name"}])
    (mock/verify-state :aggregate-store [{:id      cmd-id
                                          :version 1
                                          :name    "Test name"}])))

(deftest test-version-when-aggregate-exists
  (mock/with-mock-dal
    {:event-store [{:event-id  :event-1
                    :id        cmd-id
                    :event-seq 1
                    :meta      {}
                    :name      "Test name"}]}

    (with-redefs [dal/get-max-event-seq (fn [_]
                                          (throw (ex-info "Fetching"
                                                          {:should :not})))]
      (mock/apply-cmd ctx {:cmd-id :cmd-1
                           :id     cmd-id}))

    (mock/verify-state :event-store [{:event-id  :event-1
                                      :id        cmd-id
                                      :event-seq 1
                                      :meta      {}
                                      :name      "Test name"}
                                     {:event-id  :event-1
                                      :id        cmd-id
                                      :event-seq 2
                                      :meta      {}
                                      :name      "Test name"}])
    (mock/verify-state :aggregate-store [{:id      cmd-id
                                          :version 2
                                          :name    "Test name"}])))

(deftest test-version-when-aggregate-missing
  (mock/with-mock-dal
    (with-redefs [dal/get-max-event-seq (fn [_]
                                          (throw (ex-info "Fetching"
                                                          {:should :not})))]
      (mock/apply-cmd ctx {:cmd-id :cmd-1
                           :id     cmd-id}))

    (mock/verify-state :event-store [{:event-id  :event-1
                                      :id        cmd-id
                                      :event-seq 1
                                      :meta      {}
                                      :name      "Test name"}])
    (mock/verify-state :aggregate-store [{:id      cmd-id
                                          :version 1
                                          :name    "Test name"}])))
