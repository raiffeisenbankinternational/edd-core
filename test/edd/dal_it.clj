(ns edd.dal-it
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [edd.memory.view-store :as memory-view-store]
            [edd.elastic.view-store :as elastic-view-store]
            [edd.dal :as dal]
            [lambda.test.fixture.state :as state]
            [edd.search :as search]
            [lambda.elastic :as el]
            [lambda.uuid :as uuid]
            [clojure.string :as str]))

(def ctx
  {:elastic-search {:url (util/get-env "IndexDomainEndpoint")}
   :aws            {:region                (util/get-env "AWS_DEFAULT_REGION")
                    :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                    :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                    :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")}})
(defn load-data
  [ctx]
  (doseq [i (:aggregate-store @state/*dal-state*)]
    (log/info (el/query
                (assoc ctx
                  :method "POST"
                  :path (str "/" (:service-name ctx) "/_doc")
                  :body (util/to-json
                          i))))))

(defn test-query
  [data q]
  (binding [state/*dal-state* (atom {:aggregate-store data})]
    (let [service-name (str/replace (str "test-" (uuid/gen)) "-" "_")
          local-ctx (assoc ctx :service-name service-name)
          body {:settings
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
        (assoc local-ctx
          :method "PUT"
          :path (str "/" service-name)
          :body (util/to-json body)))
      (load-data local-ctx)
      (Thread/sleep 2000)
      (let [el-result (search/advanced-search (-> local-ctx
                                                  (elastic-view-store/register)
                                                  (assoc :query q)))
            mock-result (search/advanced-search (-> local-ctx
                                                    (memory-view-store/register)
                                                    (assoc :query q)))]
        (log/info el-result)
        (log/info mock-result)
        (el/query
          (assoc local-ctx
            :method "DELETE"
            :path (str "/" service-name)))
        [el-result mock-result]))))


