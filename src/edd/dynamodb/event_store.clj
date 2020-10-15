(ns edd.dynamodb.event-store
  (:require
    [clojure.tools.logging :as log]
    [lambda.test.fixture.state :refer [*dal-state*]]
    [edd.dal :refer [get-events
                     get-max-event-seq
                     get-sequence-number-for-id
                     get-id-for-sequence-number
                     get-aggregate-id-by-identity
                     log-dps
                     log-request
                     log-response
                     store-results]]))

(defmethod log-request
  :postgres
  [{:keys [body request-id interaction-id service-name] :as ctx}]
  )

(defmethod log-dps
  :postgres
  [{:keys [dps-resolved request-id interaction-id service-name] :as ctx}]
  )

(defmethod log-response
  :postgres
  [{:keys [resp request-id interaction-id service-name] :as ctx}]
  )



(defmethod get-sequence-number-for-id
  :postgres
  [ctx query]
  {:pre [(:id query)]}
  )

(defmethod get-id-for-sequence-number
  :postgres
  [{:keys [sequence] :as ctx}]
  {:pre [sequence]}
  )

(defmethod get-aggregate-id-by-identity
  :potgres
  [ctx identity]
  )

(defmethod get-events
  :postgres
  [{:keys [id] :as ctx}]
  )

(defmethod get-max-event-seq
  :postgres
  [ctx id]
  )



(defmethod store-results
  :dynamodb
  [ctx func]
  )


(defn register
  [ctx]
  (assoc ctx :event-store :dynamodb))
