(ns edd.dynamodb.event-store
  (:require
    [clojure.tools.logging :as log]
    [aws.dynamodb :as dynamodb]
    [lambda.test.fixture.state :refer [*dal-state*]]
    [edd.dal :refer [with-init
                     get-events
                     get-max-event-seq
                     get-sequence-number-for-id
                     get-id-for-sequence-number
                     get-aggregate-id-by-identity
                     log-dps
                     log-request
                     log-response
                     store-results]]
    [lambda.util :as util]
    [lambda.uuid :as uuid]))

(defn table-name
  [ctx table]
  (str
    (:environment-name-lower ctx)
    "-"
    (get-in ctx [:db :name])
    "-"
    (name table)
    "-ddb"))

(defmethod log-request
  :dynamodb
  [{:keys [body request-id interaction-id service-name] :as ctx}]
  ctx)

(defmethod log-dps
  :dynamodb
  [{:keys [dps-resolved request-id interaction-id service-name] :as ctx}]
  ctx)

(defmethod log-response
  :dynamodb
  [{:keys [resp request-id interaction-id service-name] :as ctx}]
  ctx)

(defmethod get-sequence-number-for-id
  :dynamodb
  [ctx query]
  {:pre [(:id query)]}
  ctx)

(defmethod get-id-for-sequence-number
  :dynamodb
  [{:keys [sequence] :as ctx}]
  {:pre [sequence]}
  ctx)

(defmethod get-aggregate-id-by-identity
  :dynamodb
  [{:keys [identity] :as ctx}]
  (let [resp (dynamodb/make-request
               (assoc ctx :action "GetItem"
                          :body {:Key       {:Id
                                             {:S (str
                                                   (:service-name ctx)
                                                   "/"
                                                   identity)}}
                                 :TableName (table-name ctx :identity-store)}))]
    (get-in resp [:Item :AggregateId :S])))

(defmethod get-events
  :dynamodb
  [{:keys [id] :as ctx}]
  (let [resp (dynamodb/make-request
               (assoc ctx :action "Query"
                          :body {:KeyConditions {:Id
                                                 {:AttributeValueList [{:S id}]
                                                  :ComparisonOperator "EQ"}}
                                 :TableName     (table-name ctx :event-store)}))]
    (map
      (fn [event]
        (util/to-edn (get-in event [:Data :S])))
      (get resp :Items []))))

(defmethod get-max-event-seq
  :dynamodb
  [{:keys [id] :as ctx}]
  (let [resp (dynamodb/make-request
               (assoc ctx :action "Query"
                          :body {:KeyConditions    {:Id
                                                    {:AttributeValueList [{:S id}]
                                                     :ComparisonOperator "EQ"}}
                                 :ScanIndexForward false
                                 :Limit            1
                                 :TableName        (table-name ctx :event-store)}))
        event (first (get resp :Items []))]
    (Integer/parseInt (get-in event [:EventSeq :N] "0"))))


(defmethod store-results
  :dynamodb
  [{:keys [resp] :as ctx}]
  (dynamodb/make-request
    (assoc ctx :action "TransactWriteItems"
               :body
               {:TransactItems
                (concat (map
                          (fn [event]
                            {:Put
                             {:Item      {"Id"            {:S (:id event)}
                                          "ItemType"      {:S :event}
                                          "Service"       {:S (keyword
                                                                (:service-name ctx))}
                                          "RequestId"     {:S (:request-id ctx)}
                                          "InteractionId" {:S (:interaction-id ctx)}
                                          "EventSeq"      {:N (str (:event-seq event))}
                                          "Data"          {:S (util/to-json event)}},
                              :TableName (table-name ctx :event-store)}})
                          (:events resp))
                        (map
                          (fn [effect]
                            {:Put
                             {:Item      {"Id"            {:S (uuid/gen)}
                                          "ItemType"      {:S :effect}
                                          "Service"       {:S (keyword
                                                                (:service-name ctx))}
                                          "TargetService" {:S (:service effect)}
                                          "RequestId"     {:S (:request-id ctx)}
                                          "InteractionId" {:S (:interaction-id ctx)}
                                          "Data"          {:S (util/to-json (assoc effect
                                                                              :request-id (:request-id ctx)
                                                                              :interaction-id (:interaction-id ctx)))}},
                              :TableName (table-name ctx :effect-store)}})
                          (:commands resp))
                        (map
                          (fn [item]
                            {:Put
                             {:Item      {"Id"            {:S (str
                                                                (:service-name ctx)
                                                                "/"
                                                                (:identity item))}
                                          "ItemType"      {:S :identity}
                                          "Service"       {:S (keyword
                                                                (:service-name ctx))}
                                          "RequestId"     {:S (:request-id ctx)}
                                          "InteractionId" {:S (:interaction-id ctx)}
                                          "AggregateId"   {:S (:id item)}
                                          "Data"          {:S (util/to-json item)}},
                              :TableName (table-name ctx :identity-store)}})
                          (:identities resp)))}))
  ctx)



(defmethod with-init
  :dynamodb
  [ctx body-fn]
  (log/debug "Initializing")
  (body-fn ctx))

(defn register
  [ctx]
  (assoc ctx :event-store :dynamodb))
