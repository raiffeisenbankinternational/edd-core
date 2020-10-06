(ns edd.aggregate-test
  (:require [clojure.tools.logging :as log]
            [edd.core :as edd]
            [edd.dal :as dal]
            [edd.el.event :as event]
            [edd.el.cmd :as cmd]
            [clojure.test :refer :all]
            [clojure.string :as str]
            [lambda.util :as util]
            [lambda.core :as core]
            [lambda.api-test :as api]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [edd.common :as common]
            [next.jdbc :as jdbc]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.test.fixture.client :refer [verify-traffic-json]]
            [edd.el.query :as query]
            [lambda.s3-test :as s3]
            [lambda.uuid :as uuid]))

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))

(def apply-ctx
  (-> {:service-name "local-test"}
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
             (assoc apply-ctx
                    :events
                    [{:event-id :event-1
                      :id       cmd-id
                      :k1       "a"}
                     {:event-id :event-2
                      :id       cmd-id
                      :k2       "b"}]) "ag1")]
    (is (= {:id            cmd-id
            :filter-result "ab"
            :e1            {:event-id :event-1,
                            :k1       "a"
                            :id       cmd-id},
            :e2            {:event-id :event-2
                            :k2       "b"
                            :id       cmd-id}}
           agg))))

(deftest test-apply-cmd
  (is (= {:error "No implementation of method: :-execute-all of protocol: #'next.jdbc.protocols/Executable found for class: nil"}
         (event/handle-event apply-ctx
                             {:apply :cmd-1}))))

(deftest test-apply-cmd-storing-error
  (with-redefs [dal/get-events (fn [ctx q]
                                 [{:event-id :event-1
                                   :id       cmd-id
                                   :k1       "a"}
                                  {:event-id :event-2
                                   :id       cmd-id
                                   :k2       "b"}])]
    (is (contains?
         (event/handle-event apply-ctx
                             {:apply :cmd-1})
         :error))))

(deftest test-apply-cmd-storing-response-error
  (with-redefs [dal/get-events (fn [ctx q]
                                 [{:event-id :event-1
                                   :id       cmd-id
                                   :k1       "a"}
                                  {:event-id :event-2
                                   :id       cmd-id
                                   :k2       "b"}])
                util/http-post (fn [url request & {:keys [raw]}]
                                 {:status 303})]
    (is (= {:error {:status 303}}
           (event/handle-event apply-ctx
                               {:apply :cmd-1})))))




