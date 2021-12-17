(ns edd.elastic.elastic-it
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.elastic.view-store :as elastic-view-store]
            [edd.memory.event-store :as memory-event-store]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]
            [edd.search :as search]
            [clojure.string :as str]
            [lambda.elastic :as el]
            [lambda.util :as util]
            [edd.response.cache :as response-cache]
            [clojure.tools.logging :as log]))
(defn get-ctx
  []
  {:elastic-search {:url (util/get-env "IndexDomainEndpoint")}
   :aws            {:region                (util/get-env "AWS_DEFAULT_REGION")
                    :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                    :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                    :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}})

(defn create-service-name
  []
  (str/replace (str (uuid/gen)) "-" "_"))

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
                       service-name)
            :body (util/to-json body)))
    ctx))

(defn cleanup-index
  [{:keys [service-name] :as ctx}]
  (el/query
   (assoc ctx
          :method "DELETE"
          :path (str "/" service-name))))

(deftest test-get-elastic-snapshot
  (let [agg-id (uuid/gen)
        ctx (-> (get-ctx)
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
                                                 (:attrs event)))))]

    (mock/apply-cmd ctx
                    {:commands [{:cmd-id :cmd-1
                                 :id     agg-id
                                 :attrs  {:my :special}}]})

    (is (= {:id      agg-id
            :version 1
            :attrs   {:my :special}}
           (search/get-snapshot ctx agg-id)))
    #_(cleanup-index ctx)))