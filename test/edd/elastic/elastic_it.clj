(ns edd.elastic.elastic-it
  (:require [clojure.test :refer [deftest is]]
            [edd.core :as edd]
            [edd.common :as common]
            [edd.elastic.view-store :as elastic-view-store]
            [edd.memory.event-store :as memory-event-store]
            [edd.test.fixture.dal :as mock]
            [edd.test.fixture.dates :as dates]
            [lambda.uuid :as uuid]
            [edd.search :as search]
            [clojure.string :as str]
            [lambda.elastic :as el]
            [lambda.util :as util]
            [edd.response.cache :as response-cache]
            [clojure.tools.logging :as log]
            [aws.ctx :as aws-ctx]
            [lambda.ctx :as lambda-ctx]))
(defn get-ctx
  []
  (-> {}
      (lambda-ctx/init)
      (aws-ctx/init)
      (elastic-view-store/register)))

(defn create-service-name
  []
  (keyword (str/replace (str (uuid/gen)) "-" "_")))

(defn create-service-index
  [{:keys [service-name] :as ctx}]
  (let [body {:settings
              {:index
               {:number_of_shards   1
                :number_of_replicas 0}}
              :mappings
              {:dynamic_templates
               [{:integers
                 {:match_mapping_type "long",
                  :mapping
                  {:type "integer",
                   :fields
                   {:number {:type "long"},
                    :keyword
                    {:type         "keyword",
                     :ignore_above 256}}}}}]}}]

    (log/info "Index name" service-name)
    (el/query
     (assoc ctx
            :method "PUT"
            :path (str "/"
                       (elastic-view-store/realm ctx)
                       "_"
                       (name service-name))
            :body (util/to-json body)))
    ctx))

(defn cleanup-index
  [{:keys [service-name] :as ctx}]
  (el/query
   (assoc ctx
          :method "DELETE"
          :path (str "/" (name service-name)))))

(def ^:private nil-id-annotations
  {:created-request-id nil
   :updated-request-id nil
   :created-user-id    nil
   :updated-user-id    nil
   :interaction-id     nil
   :invocation-id      nil})

(defn- annotations-with-date
  [created-on updated-on]
  (assoc nil-id-annotations
         :created-on created-on
         :updated-on updated-on))

(deftest test-get-elastic-snapshot
  (let [agg-id
        (uuid/gen)

        ctx
        (-> (get-ctx)
            (assoc :service-name (create-service-name)
                   :meta {:realm :test})
            (create-service-index)
            (response-cache/register-default)
            (memory-event-store/register)
            (elastic-view-store/register)
            (edd/reg-cmd :cmd-1 (fn [_ctx cmd]
                                  [{:event-id :event-1
                                    :attrs    (:attrs cmd)}]))
            (edd/reg-event :event-1 (fn [agg event]
                                      (assoc agg :attrs
                                             (:attrs event)))))

        [d1]
        (dates/with-captured-dates
          (mock/apply-cmd ctx
                          {:commands [{:cmd-id :cmd-1
                                       :id     agg-id
                                       :attrs  {:my :special}}]}))]

    (is
     (=
      {:id      agg-id
       :version 1
       :attrs   {:my :special}
       :meta    {:annotations (annotations-with-date d1 d1)}}
      (search/get-snapshot ctx agg-id)))

    (cleanup-index ctx)))

(deftest test-snapshot-diff-from-get-by-id-when-event-not-applied
  (let [agg-id
        (uuid/gen)

        ctx
        (-> (get-ctx)
            (assoc :service-name (create-service-name)
                   :meta {:realm :test})
            (create-service-index)
            (response-cache/register-default)
            (memory-event-store/register)
            (elastic-view-store/register)
            (edd/reg-cmd :cmd-1 (fn [_ cmd]
                                  [{:event-id :event-1
                                    :attrs    (:attrs cmd)}]))
            (edd/reg-event :event-1 (fn [agg event]
                                      (assoc agg :attrs
                                             (:attrs event)))))

        [d1]
        (dates/with-captured-dates
          (mock/apply-cmd ctx
                          {:commands [{:cmd-id :cmd-1
                                       :id     agg-id
                                       :attrs  {:my :special}}]}))]

    (is
     (=
      {:id      agg-id
       :version 1
       :attrs   {:my :special}
       :meta    {:annotations (annotations-with-date d1 d1)}}
      (search/get-snapshot ctx agg-id)))

    (mock/handle-cmd ctx
                     {:commands [{:cmd-id :cmd-1
                                  :id     agg-id
                                  :attrs  {:my :prop}}]})

    (is
     (=
      {:id      agg-id
       :version 1
       :attrs   {:my :special}
       :meta    {:annotations (annotations-with-date d1 d1)}}
      (search/get-snapshot ctx agg-id)))

    (is
     (=
      {:id      agg-id
       :version 2
       :attrs   {:my :prop}}
      (dissoc (common/get-by-id ctx agg-id) :meta)))

    (cleanup-index ctx)))
