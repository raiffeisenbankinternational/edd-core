(ns edd.common
  (:require
    [edd.core :as edd]
    [edd.el.event :as event]
    [edd.dal :as dal]
    [clojure.tools.logging :as log]
    [lambda.uuid :as uuid]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(defn get-by-id
  [ctx query]
  (let [resp (event/get-current-state
               (assoc ctx
                 :events (dal/get-events ctx (:id query)))
               (:id query))]
    (log/debug "Current state" resp)
    resp))

(defn get-aggregate-id-by-identity
  [ctx identity]
  (let [result
        (jdbc/execute-one! (:con ctx)
                           ["SELECT aggregate_id FROM glms.identity_store
                                           WHERE id = ?
                                           AND service = ?"
                            identity
                            (:service-name ctx)]
                           {:builder-fn rs/as-unqualified-lower-maps})]
    (log/debug "Query result" result)
    (:aggregate_id result)))

(defn get-sequence-number-for-id
  [ctx query]
  (dal/query-sequence-number-for-id ctx query))


(defn get-id-for-sequence-number
  [ctx query]
  (dal/query-id-for-sequence-number ctx query))

