(ns edd.aggregate-test
  (:require [edd.core :as edd]
            [edd.dal :as dal]
            [clojure.tools.logging :as log]
            [edd.el.event :as event]
            [clojure.test :refer [deftest testing is]]
            [lambda.util :as util]
            [edd.elastic.view-store :as elastic-view-store]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]))

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))

(def apply-ctx
  (-> mock/ctx
      (merge {:service-name "local-test"})
      (edd/reg-event
       :event-1 (fn [p v]
                  (assoc p :e1 v)))
      (edd/reg-event
       :event-2 (fn [p v]
                  (assoc p :e2 v)))
      (edd/reg-agg-filter
       (fn [{:keys [agg] :as ctx}]
         (assoc
          agg
          :filter-result
          (str (get-in agg [:e1 :k1])
               (get-in agg [:e2 :k2])))))))

(deftest test-apply
  (let [agg (event/get-current-state
             apply-ctx
             {:events
              [{:event-id  :event-1
                :id        cmd-id
                :event-seq 1
                :k1        "a"}
               {:event-id  :event-2
                :id        cmd-id
                :event-seq 2
                :k2        "b"}]
              :id "ag1"})]
    (is (= {:id            cmd-id
            :filter-result "ab"
            :version       2
            :e1            {:event-id  :event-1,
                            :k1        "a"
                            :event-seq 1
                            :id        cmd-id},
            :e2            {:event-id  :event-2
                            :k2        "b"
                            :event-seq 2
                            :id        cmd-id}}
           agg))))

#_(deftest test-apply-cmd
    (with-redefs [search/simple-search identity]

      (try
        (event/handle-event (-> apply-ctx
                                (postgres-event-store/register)
                                (assoc :apply
                                       {:aggregate-id 1})))
        (catch Exception e
          (is (= "Postgres error"
                 (.getMessage e)))))))

(deftest test-apply-cmd-storing-error
  (with-redefs [dal/get-events (fn [_]
                                 [{:event-id :event-1
                                   :id       cmd-id
                                   :k1       "a"}
                                  {:event-id :event-2
                                   :id       cmd-id
                                   :k2       "b"}])]

    (is (thrown? Exception
                 (event/handle-event (-> apply-ctx
                                         elastic-view-store/register
                                         (assoc :apply {:aggregate-id cmd-id
                                                        :apply        :cmd-1})))))))

(deftest test-apply-cmd-storing-response-error
  (with-redefs [dal/get-events (fn [_]
                                 [{:event-id :event-1
                                   :id       cmd-id
                                   :k1       "a"}
                                  {:event-id :event-2
                                   :id       cmd-id
                                   :k2       "b"}])
                util/http-get (fn [_url _request & {:keys [raw]}]
                                {:status 404})
                elastic-view-store/store-to-s3 (fn [_ctx]
                                                 nil)
                util/http-post (fn [url request & {:keys [raw]}]
                                 {:status 303
                                  :body   "Sorry"})]
    (try
      (event/handle-event (-> apply-ctx
                              elastic-view-store/register
                              (assoc
                               :apply {:aggregate-id cmd-id})))
      (catch Exception e
        (let [expected {:error {:status  303
                                :message "Sorry"}}]
          (is (= expected
                 (ex-data e)))
          (when (not= expected
                      (ex-data e))
            (log/info "Unexpected test exception" e)))))))
