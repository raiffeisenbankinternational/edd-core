(ns edd.test.fixture.execution-test
  (:require
   [clojure.test :refer [deftest is]]
   [edd.test.fixture.execution :as sut]
   [lambda.uuid :as uuid]
   [edd.memory.event-store :as event-store]
   [edd.memory.view-store :as view-store]
   [edd.test.fixture.dal :as f]
   [edd.core :as edd]
   [edd.test.fixture.dal :as mock]))

(defmulti handle (fn [ctx event] (:event-id event)))

(defmethod handle :default [ctx event])

(defn ctx []
  (-> mock/ctx))

(defmethod handle :e1
  [ctx evt]
  {:cmd-id :cmd-2
   :id     (:id evt)})

(defn handle-events [ctx events]
  (->> events
       (mapv #(handle ctx %))))

(defn register []
  (-> (ctx)
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            [{:event-id :e1
                              :name     (:cmd-id cmd)}]))
      (edd/reg-cmd :cmd-2 (fn [ctx cmd]
                            [{:event-id :e2
                              :name     (:cmd-id cmd)}]))

      (edd/reg-fx handle-events)))

(deftest test-process-cmd-response
  (f/with-mock-dal
    (let [ctx (register)
          id (uuid/gen)
          resp (sut/process-cmd-response!
                ctx
                {:commands [{:cmd-id :cmd-1
                             :id     id}]})]

      (f/verify-state :event-store
                      [{:event-id  :e1,
                        :name      :cmd-1,
                        :meta      {}
                        :id        id
                        :event-seq 1}]))))

(deftest test-process-next-on-empty-queue
  (f/with-mock-dal
    (let [ctx (register)
          id (uuid/gen)]

      (is (not (sut/process-next! ctx)))

      (f/verify-state :event-store
                      []))))

(deftest test-place-and-process-next
  (f/with-mock-dal
    (let [ctx (register)
          id1 (uuid/gen)
          id2 (uuid/gen)]
      (sut/place-cmd! {:cmd-id :cmd-1
                       :id     id1}
                      {:cmd-id :cmd-1
                       :id     id2})

      (is (sut/process-next! ctx))
      (is (= 1 (count (f/peek-state :event-store)))))))

(deftest test-place-and-process-all
  (f/with-mock-dal
    (let [ctx (register)
          id1 (uuid/gen)
          id2 (uuid/gen)]

      (sut/place-cmd! {:cmd-id :cmd-1
                       :id     id1}
                      {:cmd-id :cmd-1
                       :id     id2})

      (sut/process-all! ctx)

      (is (= 4 (count (f/peek-state :event-store)))))))

(deftest test-run-multiple-cmds
  (f/with-mock-dal
    (let [ctx (register)
          id1 (uuid/gen)
          id2 (uuid/gen)]
      (sut/run-cmd! ctx
                    {:cmd-id :cmd-1
                     :id     id1}
                    {:cmd-id :cmd-1
                     :id     id2})

      (is (= 4 (count (f/peek-state :event-store)))))))

(deftest test-run-a-single-cmd
  (f/with-mock-dal
    (let [ctx (register)
          id1 (uuid/gen)
          id2 (uuid/gen)]
      (sut/run-cmd! ctx {:cmd-id :cmd-1
                         :id     id1})

      (is (= 2 (count (f/peek-state :event-store)))))))
