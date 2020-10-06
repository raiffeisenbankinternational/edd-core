(ns edd.identity-test
  (:require
    [lambda.core :refer [handle-request]]
    [edd.el.cmd :as cmd]
    [edd.core :as edd]
    [edd.local :as local]
    [lambda.uuid :as uuid]))
(use 'clojure.test)


(defn register
  [ctx]
  (-> ctx
      (edd/reg-cmd :create-1 (fn [ctx cmd]
                               [{:identity (:name cmd)
                                 :id       (:id cmd)}
                                {:event-id :e1
                                 :name     (:name cmd)}]))
      (edd/reg-cmd :create-2 (fn [ctx cmd]
                               [nil
                                {:event-id :e2
                                 :name     "e2"}]))
      (edd/reg-cmd :create-3 (fn [ctx cmd]
                               '(nil
                                  {:event-id :e3
                                   :name     "e3"})))))

(deftest test-identity
  (let [id (uuid/gen)
        resp (cmd/get-response (register {})
                               {:commands [{:cmd-id :create-1
                                            :id     id
                                            :name   "e1"}]})]
    (is (= {:events
                        [{:event-id :e1
                          :name "e1"
                          :event-seq 1
                          :id id}],
            :identities [{:identity "e1",
                          :id       id}],
            :sequences  []
            :commands   []}
           resp))))

(deftest test-handler-returns-vector
  "Command response can contain nil, can return list or vector or map. We should handle it"
  (let [id (uuid/gen)
        resp (cmd/get-response (register {})
                               {:commands [{:cmd-id :create-2
                                            :id     id
                                            :name   "e2"}]})]
    (is (= {:events
                        [{:event-id :e2
                          :name "e2"
                          :event-seq 1
                          :id id}],
            :identities [],
            :sequences  [],
            :commands   []}
           resp))))


(deftest test-handler-returns-list
  "Command response can contain nil, can return list or vector or map. We should handle it"
  (let [id (uuid/gen)
        resp (cmd/get-response (register {})
                               {:commands [{:cmd-id :create-3
                                            :id     id
                                            :name   "e3"}]})]
    (is (= {:events
                        [{:event-id :e3
                          :name "e3"
                          :event-seq 1
                          :id id}],
            :identities [],
            :sequences  [],
            :commands   []}
           resp))))

(defn test-db
  []
  (local/local-cmd register
                   {:cmd-id :create-1
                    :id     (uuid/gen)
                    :name   "e1"}))
