(ns edd.postgres.event-store
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [lambda.uuid :as uuid]
            [clojure.string :as str]
            [edd.dal :refer [with-init
                             get-events
                             get-max-event-seq
                             get-sequence-number-for-id
                             get-id-for-sequence-number
                             get-aggregate-id-by-identity
                             get-command-response
                             log-dps
                             log-request
                             log-response
                             store-results]]
            [next.jdbc.prepare :as p]
            [edd.db :as db]
            [lambda.util :as util]
            [lambda.elastic :as elastic]))

(def errors
  {:concurrent-modification ["event_store" "pkey"
                             "duplicate key value violates unique constraint"]})

(defn error-matches?
  [msg words]
  (every?
   #(str/includes? msg %)
   words))

(defn parse-error
  [m]

  (let [match (first
               (filter
                (fn [[k v]]
                  (error-matches? m v))
                errors))]
    (if match
      {:key              (first match)
       :original-message m}
      m)))

(defn try-to-data
  [func]
  (try
    (func)
    (catch Exception e
      (log/error e "Postgres error")
      {:error (-> (.getMessage e)
                  (str/replace "\n" "")
                  (parse-error))})))

(defn breadcrumb-str [breadcrumbs]
  (str/join ":" (or breadcrumbs [])))

(defn store-event
  [ctx event]
  (log/debug "Storing event" event)
  (when event
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.event_store(id,
                                                  service_name,
                                                  invocation_id,
                                                  request_id,
                                                  interaction_id,
                                                  breadcrumbs,
                                                  event_seq,
                                                  aggregate_id,
                                                  data)
                            VALUES (?,?,?,?,?,?,?,?,?)"
                    (uuid/gen)
                    (:service-name ctx)
                    (:invocation-id ctx)
                    (:request-id ctx)
                    (:interaction-id ctx)
                    (breadcrumb-str (:breadcrumbs ctx))
                    (:event-seq event)
                    (:id event)
                    event])))

(defn store-cmd
  [ctx cmd]
  (log/debug "Storing command" cmd)
  (when (and cmd
             (> (-> cmd
                    (:commands)
                    (count))
                0))
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.command_store(id,
                                                    invocation_id,
                                                    request_id,
                                                    interaction_id,
                                                    breadcrumbs,
                                                    source_service,
                                                    target_service,
                                                    aggregate_id,
                                                    data)
                            VALUES (?,?,?,?,?,?,?,?,?)"
                    (uuid/gen)
                    (:invocation-id ctx)
                    (:request-id ctx)
                    (:interaction-id ctx)
                    (breadcrumb-str (:breadcrumbs cmd))
                    (:service-name ctx)
                    (:service cmd)
                    (-> cmd
                        (:commands)
                        (first)
                        (:id))
                    cmd])))

(defmethod log-request
  :postgres
  [{:keys [invocation-id request-id interaction-id service-name] :as ctx} body]
  (log/debug "Storing request" (:commands body))
  (let [ps (jdbc/prepare (:con ctx)
                         ["INSERT INTO glms.command_request_log(
                                                         invocation_id,
                                                         request_id,
                                                         interaction_id,
                                                         breadcrumbs,
                                                         service_name,
                                                         cmd_index,
                                                         data)
                            VALUES (?,?,?,?,?,?,?)"])
        params [[invocation-id
                 request-id
                 interaction-id
                 (breadcrumb-str (:breadcrumbs body))
                 service-name
                 0
                 body]]]
    (p/execute-batch! ps params)))

(defmethod log-dps
  :postgres
  [{:keys [dps-resolved request-id interaction-id service-name] :as ctx}]
  (log/debug "Storing deps" dps-resolved)
  #_(when dps-resolved
      (let [ps (jdbc/prepare (:con ctx)
                             ["INSERT INTO glms.command_deps_log(request_id,
                                                          interaction_id,
                                                          service_name,
                                                          cmd_index,
                                                          data)
                            VALUES (?,?,?,?,?)"])
            params (map-indexed
                    (fn [idx itm] [request-id
                                   interaction-id
                                   service-name
                                   idx
                                   itm])
                    dps-resolved)]
        (p/execute-batch! ps params))
      ctx)
  ctx)

(defn log-response-impl
  [{:keys [response-summary invocation-id request-id interaction-id service-name] :as ctx}]
  (log/debug "Storing response" response-summary)
  (when response-summary
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.command_response_log(invocation_id,
                                                           request_id,
                                                           interaction_id,
                                                           breadcrumbs,
                                                           service_name,
                                                           cmd_index,
                                                           data)
                            VALUES (?,?,?,?,?,?,?)"
                    invocation-id
                    request-id
                    interaction-id
                    (breadcrumb-str (:breadcrumbs ctx))
                    service-name
                    0,
                    response-summary])))

(defmethod log-response
  :postgres
  [ctx]
  (log-response-impl ctx))

(defn store-identity
  [ctx identity]
  (log/debug "Storing identity" identity)
  (jdbc/execute! (:con ctx)
                 ["INSERT INTO glms.identity_store(id,
                                                   invocation_id,
                                                   request_id,
                                                   interaction_id,
                                                   breadcrumbs,
                                                   service_name,
                                                   aggregate_id)
                            VALUES (?,?,?,?,?,?,?)"
                  (:identity identity)
                  (:invocation-id ctx)
                  (:request-id ctx)
                  (:interaction-id ctx)
                  (breadcrumb-str (:breadcrumbs ctx))
                  (:service-name ctx)
                  (:id identity)]))

(defn update-sequence
  [ctx sequence]
  {:pre [(:id sequence)]}
  (log/info "Update sequence" (:service-name ctx) sequence)
  (let [service-name (:service-name ctx)
        aggregate-id (:id sequence)]
    (jdbc/execute! (:con ctx)
                   ["BEGIN WORK;
                        LOCK TABLE glms.sequence_store IN EXCLUSIVE MODE;
                       UPDATE glms.sequence_store
                          SET value =  (SELECT COALESCE(MAX(value), 0) +1
                                                   FROM glms.sequence_store
                                                  WHERE service_name = ?)
                          WHERE aggregate_id = ? AND service_name = ?;
                     COMMIT WORK;"
                    service-name
                    aggregate-id
                    service-name])))

(defn prepare-store-sequence
  [ctx sequence]
  {:pre [(:id sequence)]}
  (log/info "Prepare sequence" (:service-name ctx) sequence)
  (let [service-name (:service-name ctx)
        aggregate-id (:id sequence)]
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.sequence_store (invocation_id,
                                                      request_id,
                                                      interaction_id,
                                                      breadcrumbs,
                                                      aggregate_id,
                                                      service_name,
                                                      value)
                            VALUES (?, ?, ?, ?, ?, ?, 0);"
                    (:invocation-id ctx)
                    (:request-id ctx)
                    (:interaction-id ctx)
                    (breadcrumb-str (:breadcrumbs ctx))
                    aggregate-id
                    service-name])))

(defmethod get-command-response
  :postgres
  [{:keys [request-id breadcrumbs] :as ctx}]
  {:pre [(and request-id breadcrumbs)]}
  (let [result (jdbc/execute-one!
                (:con ctx)
                ["SELECT invocation_id,
                         interaction_id,
                         cmd_index,
                         created_on,
                         data,
                         service_name
                  FROM glms.command_response_log
                  WHERE request_id = ?
                  AND breadcrumbs = ?"
                 request-id
                 (breadcrumb-str breadcrumbs)]
                {:builder-fn rs/as-unqualified-lower-maps})]
    result))

(defn get-sequence-number-for-id-imp
  [{:keys [id] :as ctx}]
  {:pre [id]}
  (let [service-name (:service-name ctx)
        value-fn #(-> (jdbc/execute-one!
                       (:con ctx)
                       ["SELECT value
                     FROM glms.sequence_store
                    WHERE aggregate_id = ?
                      AND service_name = ?"
                        id
                        service-name]
                       {:builder-fn rs/as-unqualified-lower-maps})
                      (:value))
        result (value-fn)]
    (if (= result 0)
      (do (update-sequence ctx {:id id})
          (value-fn))
      result)))

(defmethod get-sequence-number-for-id
  :postgres
  [{:keys [id] :as ctx}]
  {:pre [id]}
  (get-sequence-number-for-id-imp ctx))

(defmethod get-id-for-sequence-number
  :postgres
  [{:keys [sequence] :as ctx}]
  {:pre [sequence]}
  (let [service-name (:service-name ctx)
        result (jdbc/execute-one!
                (:con ctx)
                ["SELECT aggregate_id
                     FROM glms.sequence_store
                    WHERE value = ?
                      AND service_name = ?"
                 sequence
                 service-name]
                {:builder-fn rs/as-unqualified-lower-maps})]
    (:aggregate_id result)))

(defmethod get-aggregate-id-by-identity
  :postgres
  [{:keys [identity] :as ctx}]
  (let [result
        (jdbc/execute-one! (:con ctx)
                           ["SELECT aggregate_id FROM glms.identity_store
                                           WHERE id = ?
                                           AND service_name = ?"
                            identity
                            (:service-name ctx)]
                           {:builder-fn rs/as-unqualified-lower-maps})]
    (log/debug "Query result" result)
    (:aggregate_id result)))

(defmethod get-events
  :postgres
  [{:keys [id service-name version]
    :as   ctx
    :or   {version 0}}]
  {:pre [id service-name]}
  (log/info "Fetching events for aggregate" id)
  (let [data (try-to-data
              #(jdbc/execute! (:con ctx)
                              ["SELECT data
                                FROM glms.event_store
                                WHERE aggregate_id=?
                                  AND service_name=?
                                  AND event_seq>?
                                ORDER BY event_seq ASC" id service-name version]
                              {:builder-fn rs/as-arrays}))]
    (if (:error data)
      data
      (flatten
       (rest
        data)))))

(defmethod get-max-event-seq
  :postgres
  [{:keys [id] :as ctx}]
  (log/debug "Fetching max event-seq for aggregate" id)
  (:max
   (jdbc/execute-one! (:con ctx)
                      ["SELECT COALESCE(MAX(event_seq), 0) AS max
                         FROM glms.event_store
                         WHERE aggregate_id=?
                         AND service_name=?" id (:service-name ctx)]
                      {:builder-fn rs/as-unqualified-lower-maps})))

(defn store-events
  [ctx events]
  (log/info "Storing events")
  (doall
   (for [event (flatten events)]
     (store-event ctx event))))

(defn store-command
  [ctx cmd]
  (log/info "Storing effect")
  (log/debug "Storing effect" cmd)
  (store-cmd ctx (assoc
                  cmd
                  :request-id (:request-id ctx)
                  :interaction-id (:interaction-id ctx))))

(defn store-results-impl
  [{:keys [resp] :as ctx}]
  (log/debug "Storing results impl")
  (log-response-impl ctx)
  (store-events ctx (:events resp))
  (doseq [i (:identities resp)]
    (store-identity ctx i))

  (doseq [i (:commands resp)]
    (store-command ctx i))
  ctx)

(defmethod store-results
  :postgres
  [{:keys [resp] :as ctx}]
  (try-to-data
   #(do
      (jdbc/with-transaction
        [tx (:con ctx)]
        (store-results-impl
         (assoc ctx :con tx))
        (doseq [i (:sequences resp)]
          (prepare-store-sequence ctx i)))
      (doseq [i (:sequences resp)]
        (update-sequence ctx i))
      ctx)))

(defmethod with-init
  :postgres
  [ctx body-fn]
  (log/debug "Initializing")
  (let [db-ctx (db/init ctx)]
    (with-open [con (jdbc/get-connection (:ds db-ctx))]
      (body-fn (assoc db-ctx :con con)))))

(defn register
  [ctx]
  (assoc ctx :event-store :postgres))

(defn get-response-log
  [ctx invocation-id]
  (log/info "Fetching response" invocation-id)
  (mapv
   first
   (-> (jdbc/execute! (:con ctx)
                      ["SELECT data
                                FROM glms.command_response_log
                                WHERE invocation_id=?" invocation-id]
                      {:builder-fn rs/as-arrays})
       (rest))))

