(ns edd.postgres.event-store
  (:require [clojure.tools.logging :as log]
            [clojure.test :refer [is]]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as jdbc-conn]
            [next.jdbc.result-set :as rs]
            [lambda.uuid :as uuid]
            [lambda.util :as util]
            [clojure.string :as str]
            [edd.core :as edd]
            [edd.dal :refer [with-init
                             get-events
                             get-max-event-seq
                             get-sequence-number-for-id
                             get-id-for-sequence-number
                             get-aggregate-id-by-identity
                             get-command-response
                             log-dps
                             log-request
                             log-request-error
                             log-response
                             store-results]]
            [next.jdbc.prepare :as p]
            [edd.db :as db])
  (:import [com.zaxxer.hikari HikariDataSource]
           [com.zaxxer.hikari.pool HikariProxyConnection]))

(def errors
  {:concurrent-modification ["pkey" "duplicate key value violates unique constraint"]})

(defn ->table
  [ctx table]
  (let [realm (get-in ctx [:meta :realm] :no_realm)]
    (str " " (name realm) "." (-> table
                                  (name)
                                  (str/replace #"-" "_")) " ")))

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
      (throw (ex-info "Postgres error"
                      {:error (-> (.getMessage e)
                                  (str/replace "\n" "")
                                  (parse-error))}
                      e)))))

(defn breadcrumb-str [breadcrumbs]
  (str/join ":" (or breadcrumbs [])))

(defn breadcrumb-vec [breadcrumbs]
  (-> breadcrumbs
      (str/split #":")
      vec))

(defn store-events
  [ctx {:keys [events]}]
  (log/debug "Storing events: " events)
  (when (seq events)
    (jdbc/execute-batch! @(:con ctx)
                         (str "INSERT INTO " (->table ctx :event_store) " (id,
                                                  service_name,
                                                  invocation_id,
                                                  request_id,
                                                  interaction_id,
                                                  breadcrumbs,
                                                  event_seq,
                                                  aggregate_id,
                                                  data)
                            VALUES (?,?,?,?,?,?,?,?,?)")
                         (map
                          (fn [event]
                            [(uuid/gen)
                             (:service-name ctx)
                             (:invocation-id ctx)
                             (:request-id ctx)
                             (:interaction-id ctx)
                             (breadcrumb-str (:breadcrumbs ctx))
                             (:event-seq event)
                             (:id event)
                             event])
                          events)
                         {})))

(defmethod log-request
  :postgres
  [{:keys [invocation-id request-id interaction-id service-name] :as ctx} body]
  (log/debug "Storing request" (:commands body))
  (let [receive-count (or (-> ctx :req :attributes :ApproximateReceiveCount) "1")
        receive-count-int (Integer/parseInt receive-count)]
    (let [breadcrumbs (:breadcrumbs body)
          ps (jdbc/prepare @(:con ctx)
                           [(str "INSERT INTO " (->table ctx :command_request_log) " (
                                                         invocation_id,
                                                         request_id,
                                                         interaction_id,
                                                         breadcrumbs,
                                                         service_name,
                                                         cmd_index,
                                                         receive_count,
                                                         data)
                            VALUES (?,?,?,?,?,?,?,?)
                            ON CONFLICT (request_id, breadcrumbs) DO UPDATE
                            SET receive_count = ?")])
          params [[invocation-id
                   request-id
                   interaction-id
                   (breadcrumb-str breadcrumbs)
                   service-name
                   0
                   receive-count-int
                   (if (= breadcrumbs [0])
                     body
                     {:ref (vec
                            (drop-last breadcrumbs))})
                   receive-count-int]]]
      (p/execute-batch! ps params))))

(def ^:private updateable-fields #{"error" "receive_count"})

(defn- update-request
  [{:keys [invocation-id request-id interaction-id service-name] :as ctx} body field data]
  (when (contains? updateable-fields field)
    (let [ps (jdbc/prepare @(:con ctx)
                           [(str "UPDATE " (->table ctx :command_request_log)
                                 "SET " field " = ?
                               WHERE request_id = ?
                                 AND breadcrumbs = ?")])
          params [[data
                   request-id
                   (breadcrumb-str (:breadcrumbs body))]]]
      (p/execute-batch! ps params))))

(defmethod log-request-error
  :postgres
  [ctx body error]
  (log/debug "Storing request error" (error))
  (update-request ctx body "error" error))

(defmethod log-dps
  :postgres
  [{:keys [dps-resolved request-id interaction-id service-name] :as ctx}]
  (log/debug "Storing deps" dps-resolved)
  ctx)

(defn log-response-impl
  [{:keys [invocation-id request-id interaction-id service-name] :as ctx} summary]
  (log/debug "Storing response" summary)
  (when summary
    (jdbc/execute! @(:con ctx)
                   [(str "INSERT INTO " (->table ctx :command_response_log) " (invocation_id,
                                                           request_id,
                                                           interaction_id,
                                                           breadcrumbs,
                                                           service_name,
                                                           cmd_index,
                                                           data)
                            VALUES (?,?,?,?,?,?,?)")
                    invocation-id
                    request-id
                    interaction-id
                    (breadcrumb-str (:breadcrumbs ctx))
                    service-name
                    0,
                    summary])))

(defmethod log-response
  :postgres
  [ctx]
  (throw (ex-info "Deprecated"
                  {:message "Deprecated, done together with sore-result in transaction"})))

(defn store-identity
  [ctx identity]
  (log/debug "Storing identity" identity)
  (jdbc/execute! @(:con ctx)
                 [(str "INSERT INTO " (->table ctx :identity_store) " (id,
                                                   invocation_id,
                                                   request_id,
                                                   interaction_id,
                                                   breadcrumbs,
                                                   service_name,
                                                   aggregate_id)
                            VALUES (?,?,?,?,?,?,?)")
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
    (jdbc/execute! @(:con ctx)
                   [(str "BEGIN WORK;
                       LOCK TABLE " (->table ctx :sequence_lastval) " IN ROW EXCLUSIVE MODE;
                       INSERT INTO " (->table ctx :sequence_lastval) " AS t
                         VALUES (?, 1)
                       ON CONFLICT (service_name) DO UPDATE
                         SET last_value = t.last_value + 1;

                       UPDATE " (->table ctx :sequence_store) "
                          SET value =  (SELECT last_value
                                        FROM " (->table ctx :sequence_lastval) "
                                        WHERE service_name = ?)
                          WHERE aggregate_id = ? AND service_name = ?;
                     COMMIT WORK;")
                    service-name
                    service-name
                    aggregate-id
                    service-name])))

(defn prepare-store-sequence
  [ctx sequence]
  {:pre [(:id sequence)]}
  (log/info "Prepare sequence" (:service-name ctx) sequence)
  (let [service-name (:service-name ctx)
        aggregate-id (:id sequence)]
    (jdbc/execute! @(:con ctx)
                   [(str "INSERT INTO " (->table ctx :sequence_store) " (invocation_id,
                                                      request_id,
                                                      interaction_id,
                                                      breadcrumbs,
                                                      aggregate_id,
                                                      service_name,
                                                      value)
                            VALUES (?, ?, ?, ?, ?, ?, 0);")
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
                @(:con ctx)
                [(str "SELECT invocation_id,
                         interaction_id,
                         cmd_index,
                         created_on,
                         data,
                         service_name
                  FROM " (->table ctx :command_response_log) "
                  WHERE request_id = ?
                  AND breadcrumbs = ?")
                 request-id
                 (breadcrumb-str breadcrumbs)]
                {:builder-fn rs/as-unqualified-lower-maps})]
    result))

(defn get-sequence-number-for-id-imp
  [{:keys [id] :as ctx}]
  {:pre [id]}
  (let [service-name (:service-name ctx)
        value-fn #(-> (jdbc/execute-one!
                       @(:con ctx)
                       [(str "SELECT value
                               FROM " (->table ctx :sequence_store) "
                              WHERE aggregate_id = ?
                                AND service_name = ?")
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
                @(:con ctx)
                [(str "SELECT aggregate_id
                     FROM " (->table ctx :sequence_store) "
                    WHERE value = ?
                      AND service_name = ?")
                 sequence
                 service-name]
                {:builder-fn rs/as-unqualified-lower-maps})]
    (:aggregate_id result)))

(defmethod get-aggregate-id-by-identity
  :postgres
  [{:keys [identity] :as ctx}]
  (util/d-time
   (str "Query db for identity:" identity)
   (let [params (if (coll? identity)
                  identity
                  [identity])
         placeholders (map
                       (fn [%] "?")
                       params)
         result (if (empty? params)
                  []
                  (jdbc/execute! @(:con ctx)
                                 (concat
                                  [(str "SELECT id, aggregate_id FROM " (->table ctx :identity_store) "
                                           WHERE id IN (" (str/join ", "
                                                                    placeholders) ")
                                           AND service_name = ?")]
                                  params
                                  [(:service-name ctx)])
                                 {:builder-fn rs/as-unqualified-lower-maps}))]
     (log/debug "Query result" result)
     (let [response (if (coll? identity)
                      (reduce
                       (fn [p result]
                         (assoc p (:id result)
                                (:aggregate_id result)))
                       {}
                       result)
                      (-> result
                          (first)
                          (:aggregate_id)))]

       (log/info "Resolved identities" response)
       response))))

(defmethod get-events
  :postgres
  [{:keys [id service-name version]
    :as   ctx
    :or   {version 0}}]
  {:pre [id service-name]}
  (log/info "Fetching events for aggregate" id)
  (let [data (try-to-data
              #(jdbc/execute! @(:con ctx)
                              [(str "SELECT data
                                FROM " (->table ctx :event_store) "
                                WHERE aggregate_id=?
                                  AND service_name=?
                                  AND event_seq>?
                                ORDER BY event_seq ASC") id service-name version]
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
   (jdbc/execute-one! @(:con ctx)
                      [(str "SELECT COALESCE(MAX(event_seq), 0) AS max
                         FROM " (->table ctx :event_store) "
                         WHERE aggregate_id=?
                         AND service_name=?") id (:service-name ctx)]
                      {:builder-fn rs/as-unqualified-lower-maps})))

(def partition-size 10000)
(defn store-effects
  [ctx {:keys [effects]}]
  (log/debug "Storing effects: " effects)

  (when (seq effects)
    (loop [parts (partition partition-size
                            partition-size
                            nil
                            effects)
           total (count parts)
           current 0]
      (util/d-time
       (str "Storing effects: " current "/" total)
       (jdbc/execute-batch! @(:con ctx)
                            (str "INSERT INTO " (->table ctx :command_store) " (id,
                                                    invocation_id,
                                                    request_id,
                                                    interaction_id,
                                                    breadcrumbs,
                                                    source_service,
                                                    target_service,
                                                    aggregate_id,
                                                    data)
                            VALUES (?,?,?,?,?,?,?,?,?)")
                            (mapv
                             (fn [cmd]
                               [(uuid/gen)
                                (:invocation-id ctx)
                                (:request-id ctx)
                                (:interaction-id ctx)
                                (breadcrumb-str (:breadcrumbs ctx))
                                (:service-name ctx)
                                (:service cmd)
                                (-> cmd
                                    (:commands)
                                    (first)
                                    (:id))
                                cmd])
                             (first parts))
                            {}))
      (when (seq parts)
        (recur (rest parts)
               total
               (inc current))))))

(defn store-results-impl
  [ctx resp]
  (log/debug "Storing results impl")
  (util/d-time
   "storing-result-postgres"
   (log-response-impl ctx (:summary resp))
   (store-events ctx resp)
   (doseq [i (:identities resp)]
     (store-identity ctx i))
   (store-effects ctx resp))
  ctx)

(defmethod store-results
  :postgres
  [ctx]
  (try-to-data
   #(let [resp (:resp ctx)
          ctx (dissoc ctx :resp)
          sequences (:sequences resp)]
      (jdbc/with-transaction
        [tx @(:con ctx)]
        (store-results-impl
         (assoc ctx :con (delay tx))
         resp)
        (doseq [i sequences]
          (prepare-store-sequence ctx i)))
      (doseq [i sequences]
        (update-sequence ctx i))
      ctx)))

(defn create-pool
  ^HikariDataSource [ds-spec]
  (let [^HikariDataSource ds (jdbc-conn/->pool HikariDataSource ds-spec)]
    ds))

(def memoized-create-pool (memoize create-pool))

(defmethod with-init
  :postgres
  [ctx body-fn]
  (let [connection (delay (jdbc/get-connection
                           (memoized-create-pool (db/init ctx))))]
    (try
      (body-fn (assoc ctx :con connection))
      (catch Exception e
        (throw e))
      (finally
        (when (realized? connection)
          (let [^HikariProxyConnection conn @connection]
            (.close conn)))))))

(defn register
  [ctx]
  (assoc ctx :event-store :postgres))

(defn get-response-log
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]
        :or {breadcrumbs [0]}}]
  (log/info "Fetching response" invocation-id)
  (cond
    invocation-id (jdbc/execute!
                   @(:con ctx)
                   [(str "SELECT *
                                FROM " (->table ctx :command_response_log) "
                                WHERE invocation_id=?") invocation-id]
                   {:builder-fn rs/as-unqualified-kebab-maps})
    request-id (jdbc/execute!
                @(:con ctx)
                [(str "SELECT *
                                FROM " (->table ctx :command_response_log) "
                                WHERE request_id=?
                                AND breadcrumbs=?")
                 request-id,
                 (breadcrumb-str breadcrumbs)]
                {:builder-fn rs/as-unqualified-kebab-maps})
    interaction-id  (jdbc/execute!
                     @(:con ctx)
                     [(str "SELECT *
                                FROM " (->table ctx :command_response_log) "
                                WHERE interaction_id=?
                                AND breadcrumbs=?")
                      interaction-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps})))

(defn get-command-store
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]
        :or {breadcrumbs [0]}}]
  (log/info "Fetching response" invocation-id)
  (cond
    invocation-id (jdbc/execute!
                   @(:con ctx)
                   [(str "SELECT *
                                FROM " (->table ctx :command_store) "
                                WHERE invocation_id=?") invocation-id]
                   {:builder-fn rs/as-unqualified-kebab-maps})
    request-id (jdbc/execute!
                @(:con ctx)
                [(str "SELECT *
                                FROM " (->table ctx :command_store) "
                                WHERE request_id=?
                                AND breadcrumbs=?")
                 request-id,
                 (breadcrumb-str breadcrumbs)]
                {:builder-fn rs/as-unqualified-kebab-maps})
    interaction-id  (jdbc/execute!
                     @(:con ctx)
                     [(str "SELECT *
                                FROM " (->table ctx :command_store) "
                                WHERE interaction_id=?
                                AND breadcrumbs=?")
                      interaction-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps})))

(defn get-request-log
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]
        :or {breadcrumbs [0]}}]
  (cond
    invocation-id (jdbc/execute!
                   @(:con ctx)
                   [(str "SELECT *
                                FROM " (->table ctx :command_request_log) "
                                WHERE invocation_id=?") invocation-id]
                   {:builder-fn rs/as-unqualified-kebab-maps})
    request-id (jdbc/execute!
                @(:con ctx)
                [(str "SELECT *
                                FROM " (->table ctx :command_request_log) "
                                WHERE request_id=?
                                AND breadcrumbs=?")
                 request-id,
                 (breadcrumb-str breadcrumbs)]
                {:builder-fn rs/as-unqualified-kebab-maps})
    interaction-id  (jdbc/execute!
                     @(:con ctx)
                     [(str "SELECT *
                                FROM " (->table ctx :command_request_log) "
                                WHERE interaction_id=?
                                AND breadcrumbs=?")
                      interaction-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps})))

(defmacro verify-state
  [ctx interaction-id x y]
  `(let [query# (str "SELECT *
                    FROM " (->table ~ctx :event-store) "
                    WHERE interaction_id=?
                    ORDER BY event_seq")
         result# (edd/with-stores
                   ~ctx
                   #(-> (jdbc/execute! @(:con %)
                                       [query#
                                        ~interaction-id]
                                       {:builder-fn rs/as-unqualified-kebab-maps})))
         result# (mapv
                  #(get % :data)
                  result#)]
     (is (= ~y
            result#))))
