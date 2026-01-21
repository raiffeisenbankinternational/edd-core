(ns edd.dynamodb.event-store
  (:require
   [clojure.tools.logging :as log]
   [aws.dynamodb :as dynamodb]
   [edd.dal :refer [with-init
                    get-events
                    get-max-event-seq
                    get-command-response
                    get-aggregate-id-by-identity
                    log-request
                    log-request-error
                    log-response
                    get-records
                    store-results]]
   [lambda.util :as util]
   [clojure.string :as string]))

(def breadcrumbs-separator ":")

(defn breadcrumb-str [breadcrumbs]
  (string/join  breadcrumbs-separator (or breadcrumbs [0])))

(defn table-name
  [ctx table]
  (let [table-name (str
                    (:environment-name-lower ctx)
                    "-"
                    (get-in ctx [:db :name])
                    "-"
                    (name
                     (get-in ctx
                             [:meta :realm]
                             :prod))
                    "-"
                    (name table)
                    "-ddb")]
    table-name))

(defn data->response
  [{:keys [RequestId
           Breadcrumbs
           InvocationId
           InteractionId
           Data
           Id
           Error
           AggregateId]}]
  (cond-> {}
    RequestId (assoc :request-id (:S RequestId))
    Breadcrumbs (assoc :breadcrumbs (mapv
                                     #(Integer/parseInt %)
                                     (string/split
                                      (:S Breadcrumbs)
                                      (re-pattern breadcrumbs-separator))))
    InvocationId  (assoc :invocation-id (:S InvocationId))
    InteractionId (assoc :interaction-id (:S InteractionId))
    Data (assoc :data (util/to-edn (:S Data)))
    Id (assoc :id (:S Id))
    Error (assoc :error (util/to-edn (:S Error)))
    AggregateId (assoc :aggregate-id (:S AggregateId))))

(defn analytic-query
  [ctx
   table
   {:keys [request-id
           invocation-id
           breadcrumbs
           interaction-id]}]
  (let [resp (cond
               invocation-id
               (dynamodb/make-request
                (assoc ctx :action "Query"
                       :body
                       {:IndexName "invocation-id"
                        :KeyConditionExpression "InvocationId = :v1"
                        :ExpressionAttributeValues {":v1" {:S invocation-id}}
                        :TableName                 (table-name ctx table)}))
               request-id
               (dynamodb/make-request
                (assoc ctx :action "Query"
                       :body (if breadcrumbs
                               {:IndexName "request-id"
                                :KeyConditionExpression "RequestId = :v1 AND Breadcrumbs = :v2"
                                :ExpressionAttributeValues {":v1" {:S request-id}
                                                            ":v2" {:S
                                                                   (breadcrumb-str breadcrumbs)}}
                                :TableName                 (table-name ctx table)}
                               {:IndexName "request-id"
                                :KeyConditionExpression "RequestId = :v1"
                                :ExpressionAttributeValues {":v1" {:S request-id}}
                                :TableName                 (table-name ctx table)})))
               interaction-id
               (dynamodb/make-request
                (assoc ctx :action "Query"
                       :body {:IndexName "interaction-id"
                              :KeyConditionExpression "InteractionId = :v1"
                              :ExpressionAttributeValues {":v1" {:S interaction-id}}
                              :TableName
                              (table-name ctx table)})))]
    (mapv
     data->response
     (:Items resp))))

(defmethod log-request
  :dynamodb
  [ctx {:keys [request-id
               breadcrumbs]
        :as body}]
  (let [breadcrumbs-string (breadcrumb-str breadcrumbs)
        body (if (<= (count breadcrumbs) 1)
               body
               {:ref (vec
                      (drop-last breadcrumbs))})]
    (dynamodb/make-request
     (assoc ctx :action "BatchWriteItem"
            :body
            {:RequestItems
             {(table-name ctx :request-log)
              [{:PutRequest
                {:Item      {:Id {:S (str request-id ":" breadcrumbs-string)}
                             :RequestId     {:S (:request-id ctx)}
                             :Breadcrumbs   {:S breadcrumbs-string}
                             :InteractionId {:S (:interaction-id ctx)}
                             :InvocationId  {:S (:invocation-id ctx)}
                             :Data          {:S  (util/to-json body)}}}}]}}))))

(defn update-request-log
  [{:keys [request-id
           breadcrumbs]
    :as ctx}
   _body
   error]
  (let [breadcumbs (breadcrumb-str breadcrumbs)]
    (dynamodb/make-request
     (assoc ctx :action "PutItem"
            :body
            {:TableName (table-name ctx :request-log)
             :Item {:Id {:S (str request-id ":" breadcumbs)}
                    :RequestId     {:S (:request-id ctx)}
                    :Breadcrumbs   {:S (breadcrumb-str breadcumbs)}
                    :InteractionId {:S (:interaction-id ctx)}
                    :InvocationId  {:S (:invocation-id ctx)}
                    :Error          {:S  (util/to-json error)}}}))))

(defmethod log-request-error
  :dynamodb
  [ctx body error]
  (update-request-log ctx body error))

(defmethod log-response
  :dynamodb
  [ctx]
  ctx)

(defmethod get-command-response
  :dynamodb
  [{:keys [request-id breadcrumbs] :as ctx}]
  {:pre [(and request-id breadcrumbs)]}
  (let [results (analytic-query ctx
                                :response-log
                                {:request-id request-id
                                 :breadcrumbs breadcrumbs})]
    (first results)))

(defn query-identity
  [ctx identity]
  (let [single (string? identity)
        values (if single
                 [identity]
                 (vec identity))
        query-resp (mapv
                    #(dynamodb/make-request
                      (assoc ctx :action "Query"
                             :body {:KeyConditionExpression "Id =  :v"
                                    :ConsistentRead true
                                    :ExpressionAttributeValues
                                    {":v"
                                     {:S (str
                                          (name (:service-name ctx))
                                          "/"
                                          %)}}
                                    :TableName (table-name ctx :identity-store)}))
                    values)
        query-resp (mapv
                    #(get-in % [:Items 0 :AggregateId :S])
                    query-resp)]

    (if single
      (first query-resp)
      (reduce-kv
       (fn [p idx v]

         (if-not v
           p
           (assoc p
                  (get values idx)
                  v)))
       {}
       query-resp))))

(defmethod get-aggregate-id-by-identity
  :dynamodb
  [{:keys [identity] :as ctx}]
  (let [resp (cond
               (nil? identity) nil
               (and
                (not
                 (string? identity))
                (not
                 (seq identity))) {}
               :else (query-identity ctx identity))]
    resp))

(defmethod get-events
  :dynamodb
  [{:keys [id version
           request-id
           breadcrumbs
           interaction-id
           invocation-id]
    :as   ctx
    :or   {version 0}}]
  (log/info (format "Fetching events for: %s, starting version: %s"
                    id version))
  (if id
    (do
      (util/d-time
       (format "Fetching events for: %s, starting version: %s" id version)
       (let [resp (dynamodb/make-request
                   (assoc ctx :action "Query"
                          :body {:KeyConditions {:AggregateId
                                                 {:AttributeValueList [{:S id}]
                                                  :ComparisonOperator "EQ"}
                                                 :EventSeq
                                                 {:AttributeValueList [{:N (str version)}]
                                                  :ComparisonOperator "GT"}}
                                 :TableName     (table-name ctx :event-store)}))
             events (map
                     (fn [event]
                       (util/to-edn (get-in event [:Data :S])))
                     (get resp :Items []))]

         (log/info (format "Received events: %s" (count events)))
         events)))
    (do "Fetching by request data"
        (analytic-query ctx
                        :event-store
                        (cond-> {}
                          request-id (assoc :request-id request-id)
                          breadcrumbs (assoc :breadcrumbs request-id)
                          interaction-id (assoc :interaction-id request-id)
                          invocation-id (assoc :invocation-id request-id))))))

(defmethod get-max-event-seq
  :dynamodb
  [{:keys [id] :as ctx}]
  (let [resp (dynamodb/make-request
              (assoc ctx :action "Query"
                     :body {:KeyConditions    {:AggregateId
                                               {:AttributeValueList [{:S id}]
                                                :ComparisonOperator "EQ"}}
                            :ScanIndexForward false
                            :Limit            1
                            :TableName        (table-name ctx :event-store)}))
        event (first (get resp :Items []))]
    (Integer/parseInt (get-in event [:EventSeq :N] "0"))))

(defn create-effect-id
  [request-id breadcrumbs]
  (str request-id
       "-"
       (breadcrumb-str
        breadcrumbs)))

(defmethod store-results
  :dynamodb
  [{:keys [resp] :as ctx}]
  (let [items (concat (map
                       (fn [event]
                         {:Put
                          {:Item      {"AggregateId"   {:S (:id event)}
                                       "ItemType"      {:S :event}
                                       "Service"       {:S (keyword
                                                            (:service-name ctx))}
                                       "RequestId"     {:S (:request-id ctx)}
                                       "Breadcrumbs"   {:S (breadcrumb-str
                                                            (:breadcrumbs ctx))}
                                       "InteractionId" {:S (:interaction-id ctx)}
                                       "InvocationId"  {:S (:invocation-id ctx)}
                                       "EventSeq"      {:N (str (:event-seq event))}
                                       "Data"          {:S (util/to-json event)}},
                           :ConditionExpression "attribute_not_exists(Id)"
                           :TableName (table-name ctx :event-store)}})
                       (:events resp))
                      (map
                       (fn [effect]
                         {:Put
                          {:Item      {"Id"            {:S
                                                        (create-effect-id
                                                         (:request-id ctx)
                                                         (:breadcrumbs effect))}
                                       "ItemType"      {:S :effect}
                                       "Service"       {:S (keyword
                                                            (:service-name ctx))}
                                       "TargetService" {:S (:service effect)}
                                       "RequestId"     {:S (:request-id ctx)}
                                       "Breadcrumbs"   {:S (breadcrumb-str
                                                            (:breadcrumbs ctx))}
                                       "InteractionId" {:S (:interaction-id ctx)}
                                       "InvocationId"  {:S (:invocation-id ctx)}
                                       "Data"          {:S (util/to-json (assoc effect
                                                                                :request-id (:request-id ctx)
                                                                                :interaction-id (:interaction-id ctx)))}},
                           :ConditionExpression "attribute_not_exists(Id)"
                           :TableName (table-name ctx :effect-store)}})
                       (:effects resp))
                      (map
                       (fn [item]
                         {:Put
                          {:Item      {"Id" {:S (str
                                                 (name
                                                  (:service-name ctx))
                                                 "/"
                                                 (:identity item))}
                                       "ItemType"      {:S :identity}
                                       "Service"       {:S (keyword
                                                            (:service-name ctx))}
                                       "RequestId"     {:S (:request-id ctx)}
                                       "Breadcrumbs"   {:S (breadcrumb-str
                                                            (:breadcrumbs ctx))}
                                       "InteractionId" {:S (:interaction-id ctx)}
                                       "InvocationId"  {:S (:invocation-id ctx)}
                                       "AggregateId"   {:S (:id item)}
                                       "Data"          {:S (util/to-json item)}},
                           :ConditionExpression "attribute_not_exists(Id)"
                           :TableName (table-name ctx :identity-store)}})
                       (:identities resp))
                       ;; Only log response when summary exists (matches postgres implementation)
                      (when (:summary resp)
                        [{:Put
                          {:Item      {"Id"            {:S (str
                                                            (:request-id ctx)
                                                            ":"
                                                            (breadcrumb-str
                                                             (:breadcrumbs ctx)))}
                                       "Service"       {:S (keyword
                                                            (:service-name ctx))}
                                       "RequestId"     {:S (:request-id ctx)}
                                       "Breadcrumbs"   {:S (breadcrumb-str
                                                            (:breadcrumbs ctx))}
                                       "InteractionId" {:S (:interaction-id ctx)}
                                       "InvocationId" {:S (:invocation-id ctx)}
                                       "Data"          {:S (util/to-json
                                                            (:summary resp))}},
                           :ConditionExpression "attribute_not_exists(Id)"
                           :TableName (table-name ctx :response-log)}}]))]
    (when-not (empty? items)
      (dynamodb/make-request
       (assoc ctx :action "TransactWriteItems"
              :body
              {:TransactItems items}))))
  ctx)

(defmethod get-records
  :dynamodb
  [ctx {:keys [interaction-id]}]
  (let [events (analytic-query ctx
                               :event-store
                               {:interaction-id interaction-id})
        effects (analytic-query ctx
                                :effect-store
                                {:interaction-id interaction-id})]
    {:events  (mapv :data events)
     :effects (mapv :data effects)}))

(defmethod with-init
  :dynamodb
  [ctx body-fn]
  (log/debug "Initializing")
  (body-fn ctx))

(defn register
  [ctx]
  (let [db-name
        (or (util/get-env "ApplicationName")
            (get-in ctx [:db :name]))]
    (-> ctx
        (assoc :edd-event-store :dynamodb)
        (assoc-in [:db :name] db-name))))
