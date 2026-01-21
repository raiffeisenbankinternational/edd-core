(ns edd.postgres.event-store
  (:require [clojure.tools.logging :as log]
            [clojure.test :refer [is]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [lambda.uuid :as uuid]
            [lambda.util :as util]
            [clojure.string :as string]
            [edd.postgres.history :as history]
            [edd.core :as edd]
            [edd.postgres.pool :as pool :refer [*DB*]]
            [edd.dal :refer [with-init
                             get-events
                             get-max-event-seq
                             get-aggregate-id-by-identity
                             get-command-response
                             log-request
                             log-request-error
                             log-response
                             get-records
                             store-results]]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def errors
  {:concurrent-modification ["pkey" "duplicate key value violates unique constraint"]})

(defn ->table
  [ctx table]
  (let [realm (get-in ctx [:meta :realm] :no_realm)]
    (str " " (name realm) "." (-> table
                                  (name)
                                  (string/replace #"-" "_")) " ")))

(defn error-matches?
  [msg words]
  (every?
   #(string/includes? msg %)
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
                      {:error (-> (ex-message e)
                                  (or "No message")
                                  (string/replace "\n" "")
                                  (parse-error))}
                      e)))))

(defn breadcrumb-str [breadcrumbs]
  (string/join ":" (or breadcrumbs [])))

(defn store-events
  [ctx {:keys [events]}]
  (log/debug "Storing events: " events)
  (when (seq events)
    (jdbc/execute-batch! *DB*
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
  (let [receive-count (or
                       (-> ctx :attributes :ApproximateReceiveCount)
                       "-1")
        receive-count-int (Integer/parseInt receive-count)
        breadcrumbs (:breadcrumbs body)]
    (jdbc/execute! *DB*
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
                            SET receive_count = command_request_log.receive_count + 1")
                    invocation-id
                    request-id
                    interaction-id
                    (breadcrumb-str breadcrumbs)
                    service-name
                    0
                    receive-count-int
                    (if (= breadcrumbs [0])
                      body
                      {:ref (vec
                             (drop-last breadcrumbs))})])))

(defn get-parent-breadcrumbs
  [breadcrumbs]
  (->> breadcrumbs
       drop-last
       (reduce
        (fn [p v]
          (let [current (conj (or (last p)
                                  [])
                              v)]
            (conj p current)))
        [])
       reverse))

(defn log-request-error-impl
  [{:keys [request-id] :as ctx} body data]
  (let [breadcrumbs (:breadcrumbs body)]
    (jdbc/execute! *DB*
                   [(str "UPDATE " (->table ctx :command_request_log) "
                             SET error = ?,
                                 fx_exception = fx_exception + 1
                           WHERE request_id = ?
                             AND breadcrumbs = ?")
                    data
                    request-id
                    (breadcrumb-str breadcrumbs)])))

(defmethod log-request-error
  :postgres
  [ctx body error]
  (util/d-time
   (format "Postgres storing request error, request-id: %s" (:request-id ctx))
   (log-request-error-impl ctx body error)))

(defn log-response-impl
  [{:keys [invocation-id
           request-id
           interaction-id
           service-name
           breadcrumbs] :as ctx}
   {:keys [summary
           effects
           error]}]
  (log/debug "Storing response" summary)
  (when summary
    (jdbc/execute! *DB*
                   [(str "INSERT INTO " (->table ctx :command_response_log) "
                                          (invocation_id,
                                           request_id,
                                           interaction_id,
                                           breadcrumbs,
                                           service_name,
                                           cmd_index,
                                           fx_processed,
                                           fx_created,
                                           fx_error,
                                           data)
                            VALUES (?,?,?,?,?,?,?,?,?,?)")
                    invocation-id
                    request-id
                    interaction-id
                    (breadcrumb-str breadcrumbs)
                    service-name
                    0
                    (cond
                                        ; First one is on actuall fx
                      (= (count breadcrumbs) 1) 0
                                        ; I Would not call error one processed
                      error 0
                      :else 1)
                    (count effects)
                    (if error
                      1
                      0)
                    summary])))

(defmethod log-response
  :postgres
  [ctx]
  (throw (ex-info "Deprecated"
                  {:message "Deprecated, done together with sore-result in transaction"})))

(defn store-identity
  [ctx identity]
  (log/debug "Storing identity" identity)
  (jdbc/execute! *DB*
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

(defmethod get-command-response
  :postgres
  [{:keys [request-id breadcrumbs] :as ctx}]
  {:pre [(and request-id breadcrumbs)]}
  (let [result (jdbc/execute-one!
                *DB*
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
                  (jdbc/execute! *DB*
                                 (concat
                                  [(str "SELECT id, aggregate_id FROM " (->table ctx :identity_store) "
                                           WHERE id IN (" (string/join ", "
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
    :as   ctx}]
  {:pre [id service-name]}
  (let [version (or version
                    0)]
    (util/d-time
     (str "Fetching events for aggregate: "  {:id id
                                              :version version})
     (let [data (jdbc/execute! *DB*
                               [(str "SELECT data
                                FROM " (->table ctx :event_store) "
                                WHERE aggregate_id=?
                                  AND service_name=?
                                  AND event_seq>?
                                ORDER BY event_seq ASC")
                                id
                                service-name
                                version]
                               {:builder-fn rs/as-arrays})]
       (if (:error data)
         data
         (flatten
          (rest
           data)))))))

(defmethod get-max-event-seq
  :postgres
  [{:keys [id] :as ctx}]
  (log/debug "Fetching max event-seq for aggregate" id)
  (:max
   (jdbc/execute-one! *DB*
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
           current 1]
      (util/d-time
       (str "Storing effects: " current "/" total)
       (jdbc/execute-batch! *DB*
                            (str "INSERT INTO " (->table ctx :command_store) " (id,
                                                    invocation_id,
                                                    request_id,
                                                    interaction_id,
                                                    breadcrumbs,
                                                    source_service,
                                                    target_service,
                                                    target_breadcrumbs,
                                                    aggregate_id,
                                                    data)
                            VALUES (?,?,?,?,?,?,?,?,?,?)")
                            (mapv
                             (fn [cmd]
                               [(uuid/gen)
                                (:invocation-id ctx)
                                (:request-id ctx)
                                (:interaction-id ctx)
                                (breadcrumb-str (:breadcrumbs ctx))
                                (:service-name ctx)
                                (:service cmd)
                                (breadcrumb-str (:breadcrumbs cmd))
                                (-> cmd
                                    (:commands)
                                    (first)
                                    (:id))
                                cmd])
                             (first parts))
                            {}))
      (when (seq (rest parts))
        (recur (rest parts)
               total
               (inc current))))))

(defn -compare-version ^long
  [aggregate snapshot]
  (letfn [(get-version [agg] (:version agg))]
    (log/infof "compare aggregate version %d to snapshot version %d"
               (get-version aggregate)
               (get-version snapshot))
    (long (compare (get-version aggregate) (get-version snapshot)))))

(defn store-history
  [ctx resp]
  (let [{:keys [history]}
        resp

        tx
        (comp
         (filter
          (fn [item]
            (let [{:keys [aggregate snapshot]} item
                  {:keys [id]} aggregate
                  cmp (-compare-version aggregate snapshot)]
              (or (pos? cmp)
                  (log/infof "Removing aggregate due to -compare-version result: %s, id: %s, snapshot: %s"
                             cmp id snapshot))))))

        aggregates
        (into [] tx history)]

    (when (seq aggregates)
      (history/new-entries ctx aggregates))))

(defn store-results-impl
  [ctx resp]
  (util/d-time
   "storing-result-postgres "
   (log-response-impl ctx resp)
   (store-events ctx resp)
   (store-history ctx resp)
   (doseq [i (:identities resp)]
     (store-identity ctx i))
   (store-effects ctx resp))
  ctx)

(defmethod store-results
  :postgres
  [ctx]
  (try-to-data
   #(let [resp
          (:resp ctx)

          ctx
          (dissoc ctx :resp)]

      (pool/with-tx []
        (store-results-impl ctx resp))
      ctx)))

(defmethod with-init
  :postgres
  [ctx body-fn]
  (pool/with-init ctx body-fn))

(defn register
  [ctx]
  (assoc ctx :edd-event-store :postgres))

(defmethod get-records
  :postgres
  [ctx {:keys [interaction-id]}]
  {:pre [interaction-id]}
  (util/d-time
   (format "Postgres get-records for interaction-id: %s" interaction-id)
   (let [events (jdbc/execute!
                 *DB*
                 [(str "SELECT data
                        FROM " (->table ctx :event_store) "
                        WHERE interaction_id=?
                        ORDER BY event_seq ASC")
                  interaction-id]
                 {:builder-fn rs/as-arrays})
         effects (jdbc/execute!
                  *DB*
                  [(str "SELECT data
                         FROM " (->table ctx :command_store) "
                         WHERE interaction_id=?
                         ORDER BY id")
                   interaction-id]
                  {:builder-fn rs/as-unqualified-kebab-maps})]
     {:events (flatten (rest events))
      :effects (mapv :data effects)})))

(defn get-response-log
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]}]
  (cond
    invocation-id (util/d-time
                   (format "Postgres get-response-log by invocation-id: %s" invocation-id)
                   (jdbc/execute!
                    *DB*
                    [(str "SELECT *
                                 FROM " (->table ctx :command_response_log) "
                                 WHERE invocation_id=?") invocation-id]
                    {:builder-fn rs/as-unqualified-kebab-maps}))
    request-id (if breadcrumbs
                 (util/d-time
                  (format "Postgres get-response-log by request-id: %s, breadcrumbs: %s" request-id breadcrumbs)
                  (jdbc/execute!
                   *DB*
                   [(str "SELECT *
                                 FROM " (->table ctx :command_response_log) "
                                 WHERE request_id=?
                                 AND breadcrumbs=?")
                    request-id,
                    (breadcrumb-str breadcrumbs)]
                   {:builder-fn rs/as-unqualified-kebab-maps}))
                 (util/d-time
                  (format "Postgres get-response-log by request-id: %s" request-id)
                  (jdbc/execute!
                   *DB*
                   [(str "SELECT *
                                 FROM " (->table ctx :command_response_log) "
                                 WHERE request_id=?")
                    request-id]
                   {:builder-fn rs/as-unqualified-kebab-maps})))
    interaction-id (util/d-time
                    (format "Postgres get-response-log by interaction-id: %s" interaction-id)
                    (jdbc/execute!
                     *DB*
                     [(str "SELECT *
                                 FROM " (->table ctx :command_response_log) "
                                 WHERE interaction_id=?
                                 AND breadcrumbs=?")
                      interaction-id,
                      (breadcrumb-str (or breadcrumbs
                                          [0]))]
                     {:builder-fn rs/as-unqualified-kebab-maps}))))

(defn get-command-store
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]
        :or {breadcrumbs [0]}}]
  (cond
    invocation-id (util/d-time
                   (format "Postgres get-command-store by invocation-id: %s" invocation-id)
                   (jdbc/execute!
                    *DB*
                    [(str "SELECT *
                                 FROM " (->table ctx :command_store) "
                                 WHERE invocation_id=?") invocation-id]
                    {:builder-fn rs/as-unqualified-kebab-maps}))
    request-id (util/d-time
                (format "Postgres get-command-store by request-id: %s, breadcrumbs: %s" request-id breadcrumbs)
                (jdbc/execute!
                 *DB*
                 [(str "SELECT *
                                 FROM " (->table ctx :command_store) "
                                 WHERE request_id=?
                                 AND breadcrumbs=?")
                  request-id,
                  (breadcrumb-str breadcrumbs)]
                 {:builder-fn rs/as-unqualified-kebab-maps}))
    interaction-id (util/d-time
                    (format "Postgres get-command-store by interaction-id: %s" interaction-id)
                    (jdbc/execute!
                     *DB*
                     [(str "SELECT *
                                 FROM " (->table ctx :command_store) "
                                 WHERE interaction_id=?
                                 AND breadcrumbs=?")
                      interaction-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps}))))

(defn get-request-log
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]
        :or {breadcrumbs [0]}}]
  (cond
    invocation-id (jdbc/execute!
                   *DB*
                   [(str "SELECT *
                                FROM " (->table ctx :command_request_log) "
                                WHERE invocation_id=?") invocation-id]
                   {:builder-fn rs/as-unqualified-kebab-maps})
    request-id (jdbc/execute!
                *DB*
                [(str "SELECT *
                                FROM " (->table ctx :command_request_log) "
                                WHERE request_id=?
                                AND breadcrumbs=?")
                 request-id,
                 (breadcrumb-str breadcrumbs)]
                {:builder-fn rs/as-unqualified-kebab-maps})
    interaction-id  (jdbc/execute!
                     *DB*
                     [(str "SELECT *
                                FROM " (->table ctx :command_request_log) "
                                WHERE interaction_id=?
                                AND breadcrumbs=?")
                      interaction-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps})))

(defn get-response-trace-log
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]}]
  (let [select "SUM(fx_created) AS fx_created,
                SUM(fx_processed) AS fx_processed,
                SUM(fx_error) AS fx_error,
                SUM(fx_created) - SUM(fx_processed) - SUM(fx_error) AS fx_remaining"]
    (cond
      invocation-id (util/d-time
                     (format "Postgres get-response-trace-log by invocation-id: %s" invocation-id)
                     (jdbc/execute!
                      *DB*
                      [(str "SELECT " select  "
                                  FROM " (->table ctx :command_response_log) "
                                  WHERE invocation_id=?") invocation-id]
                      {:builder-fn rs/as-unqualified-kebab-maps}))
      request-id (if breadcrumbs
                   (util/d-time
                    (format "Postgres get-response-trace-log by request-id: %s, breadcrumbs: %s" request-id breadcrumbs)
                    (jdbc/execute!
                     *DB*
                     [(str "SELECT " select  "
                                 FROM " (->table ctx :command_response_log) "
                                 WHERE request_id=?
                                 AND breadcrumbs=?")
                      request-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps}))
                   (util/d-time
                    (format "Postgres get-response-trace-log by request-id: %s" request-id)
                    (jdbc/execute!
                     *DB*
                     [(str "SELECT " select  "
                                 FROM " (->table ctx :command_response_log) "
                                 WHERE request_id=?")
                      request-id]
                     {:builder-fn rs/as-unqualified-kebab-maps})))
      interaction-id (util/d-time
                      (format "Postgres get-response-trace-log by interaction-id: %s" interaction-id)
                      (jdbc/execute!
                       *DB*
                       [(str "SELECT " select  "
                                    FROM " (->table ctx :command_response_log) "
                                    WHERE interaction_id=?
                                    AND breadcrumbs=?")
                        interaction-id,
                        (breadcrumb-str (or breadcrumbs
                                            [0]))]
                       {:builder-fn rs/as-unqualified-kebab-maps})))))

(defn get-request-trace-log
  [ctx {:keys [request-id
               invocation-id
               breadcrumbs
               interaction-id]}]
  (let [select "SUM(fx_exception) AS fx_exception"]
    (cond
      invocation-id (util/d-time
                     (format "Postgres get-request-trace-log by invocation-id: %s" invocation-id)
                     (jdbc/execute!
                      *DB*
                      [(str "SELECT " select  "
                                  FROM " (->table ctx :command_request_log) "
                                  WHERE invocation_id=?") invocation-id]
                      {:builder-fn rs/as-unqualified-kebab-maps}))
      request-id (if breadcrumbs
                   (util/d-time
                    (format "Postgres get-request-trace-log by request-id: %s, breadcrumbs: %s" request-id breadcrumbs)
                    (jdbc/execute!
                     *DB*
                     [(str "SELECT " select  "
                                 FROM " (->table ctx :command_request_log) "
                                 WHERE request_id=?
                                 AND breadcrumbs=?")
                      request-id,
                      (breadcrumb-str breadcrumbs)]
                     {:builder-fn rs/as-unqualified-kebab-maps}))
                   (util/d-time
                    (format "Postgres get-request-trace-log by request-id: %s" request-id)
                    (jdbc/execute!
                     *DB*
                     [(str "SELECT " select  "
                                 FROM " (->table ctx :command_request_log) "
                                 WHERE request_id=?")
                      request-id]
                     {:builder-fn rs/as-unqualified-kebab-maps})))
      interaction-id (util/d-time
                      (format "Postgres get-request-trace-log by interaction-id: %s" interaction-id)
                      (jdbc/execute!
                       *DB*
                       [(str "SELECT " select  "
                                    FROM " (->table ctx :command_request_log) "
                                    WHERE interaction_id=?
                                    AND breadcrumbs=?")
                        interaction-id,
                        (breadcrumb-str (or breadcrumbs
                                            [0]))]
                       {:builder-fn rs/as-unqualified-kebab-maps})))))

(defmacro verify-state
  [ctx interaction-id x y]
  `(let [query# (str "SELECT *
                    FROM " (->table ~ctx :event-store) "
                    WHERE interaction_id=?
                    ORDER BY event_seq")
         result# (edd/with-stores
                   ~ctx
                   (fn [_#]
                     (jdbc/execute! *DB*
                                    [query# ~interaction-id]
                                    {:builder-fn rs/as-unqualified-kebab-maps})))
         result# (mapv
                  #(get % :data)
                  result#)]
     (is (= ~y
            result#))))
