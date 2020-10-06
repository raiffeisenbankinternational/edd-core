(ns edd.sequence-test
  (:require
   [lambda.core :refer [handle-request]]
   [edd.el.cmd :as cmd]
   [edd.core :as edd]
   [edd.core-test :as edd-test]
   [edd.local :as local]
   [lambda.uuid :as uuid]))
(use 'clojure.test)

(defn register [ctx]
  (-> ctx
      (edd/reg-cmd :create-1 (fn [ctx cmd]
                               [{:sequence :limit-application
                                 :id       (:id cmd)}
                                {:event-id :e1
                                 :name     (:name cmd)}]))))

(deftest test-sequence-generation
  (let [id (uuid/gen)
        resp (cmd/get-response (register {})
                               {:commands [{:cmd-id :create-1
                                            :id     id
                                            :name   "e1"}]})]
    (is (= {:events     [{:event-id  :e1
                          :name      "e1"
                          :id        id
                          :event-seq 1}],
            :identities [],
            :sequences  [{:sequence :limit-application
                          :id       id}]
            :commands   []}
           resp))))
