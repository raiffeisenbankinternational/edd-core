(ns edd.dal
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [lambda.uuid :as uuid]
            [clojure.string :as str]
            [next.jdbc.prepare :as p]
            [edd.db :as db]
            [lambda.util :as util]
            [lambda.elastic :as elastic]))

(def errors
  {:unique "duplicate key value violates unique constraint"})

(defn try-to-data
  [func]
  (try
    (func)
    (catch Exception e
      (log/error e "Postgres error")
      {:error (-> (.getMessage e)
                  (str/replace "\n" ""))})))

(defn read-realm
  [ctx]
  (jdbc/execute-one!
   (:con ctx)
   ["SELECT * FROM glms.realm LIMIT 1"]
   {:builder-fn rs/as-unqualified-lower-maps}))

(defn store-event
  [ctx realm event]
  (log/debug "Storing event" event)
  (when event
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.event_store(id, service, request_id, interaction_id, event_seq, aggregate_id, realm_id, data)
                            VALUES (?,?,?,?,?,?,?,?)"
                    (uuid/gen)
                    (:service-name ctx)
                    (:request-id ctx)
                    (:interaction-id ctx)
                    (:event-seq event)
                    (:id event)
                    (:id realm)
                    event])))

(defn store-cmd
  [ctx cmd]
  (log/debug "Storing command" cmd)
  (when cmd
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.command_store(id, service, data)
                            VALUES (?,?,?)"
                    (uuid/gen)
                    (name (:service cmd))
                    cmd])))

(defn log-request
  [{:keys [body request-id interaction-id service-name] :as ctx}]
  (log/debug "Storing request" (:commands body))
  (when (:commands body)
    (let [ps (jdbc/prepare (:con ctx)
                           ["INSERT INTO glms.command_request_log(request_id,
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
                  (:commands body))]
      (p/execute-batch! ps params))))

(defn log-dps
  [{:keys [dps-resolved request-id interaction-id service-name] :as ctx}]
  (log/debug "Storing deps" dps-resolved)
  (when dps-resolved
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
    ctx))

(defn log-response
  [{:keys [resp request-id interaction-id service-name] :as ctx}]
  (log/debug "Storing response" resp)
  (when resp
    (jdbc/execute! (:con ctx)
                   ["INSERT INTO glms.command_response_log(request_id,
                                                           interaction_id,
                                                           service_name,
                                                           data)
                            VALUES (?,?,?,?)"
                    request-id
                    interaction-id
                    service-name
                    resp])))

(defn store-identity
  [ctx identity]
  (log/debug "Storing identity" identity)
  (jdbc/execute! (:con ctx)
                 ["INSERT INTO glms.identity_store(id, service, aggregate_id)
                            VALUES (?,?,?)"
                  (:identity identity)
                  (name (:service-name ctx))
                  (:id identity)]))

(defn store-sequence
  [ctx sequence]
  {:pre [(:id sequence)]}
  (log/debug "Storing sequence" sequence)
  (let [service-name (:service-name ctx)
        aggregate-id (:id sequence)]
    (jdbc/execute! (:con ctx)
                   ["BEGIN WORK;
                        LOCK TABLE glms.sequence_store IN EXCLUSIVE MODE;
                       INSERT INTO glms.sequence_store (aggregate_id, service_name, value)
                            VALUES (?, ?, (SELECT COALESCE(MAX(value), 0) +1
                                             FROM glms.sequence_store
                                            WHERE service_name = ?));
                    COMMIT WORK;"
                    aggregate-id
                    service-name
                    service-name])))

(defn query-sequence-number-for-id
  [ctx query]
  {:pre [(:id query)]}
  (let [service-name (:service-name ctx)
        result (jdbc/execute-one!
                (:con ctx)
                ["SELECT value
                     FROM glms.sequence_store
                    WHERE aggregate_id = ?
                      AND service_name = ?"
                 (:id query)
                 service-name]
                {:builder-fn rs/as-unqualified-lower-maps})]
    (:value result)))

(defn query-id-for-sequence-number
  [ctx query]
  {:pre [(:value query)]}
  (let [service-name (:service-name ctx)
        result (jdbc/execute-one!
                (:con ctx)
                ["SELECT aggregate_id
                     FROM glms.sequence_store
                    WHERE value = ?
                      AND service_name = ?"
                 (:value query)
                 service-name]
                {:builder-fn rs/as-unqualified-lower-maps})]
    (:aggregate_id result)))

(defn get-events
  [ctx id]
  (log/debug "Fetching events for aggregate" id)
  (let [data (try-to-data
               #(jdbc/execute! (:con ctx)
                               ["SELECT data
                         FROM glms.event_store
                        WHERE aggregate_id=?
                     ORDER BY event_seq ASC" id]
                               {:builder-fn rs/as-arrays}))]
    (if (:error data)
      data
      (flatten
       (rest
         data)))))

(defn get-max-event-seq
  [ctx id]
  (log/debug "Fetching max event-seq for aggregate" id)
  (:max
   (jdbc/execute-one! (:con ctx)
                      ["SELECT COALESCE(MAX(event_seq), 0) AS max
                         FROM glms.event_store WHERE aggregate_id=?" id]
                      {:builder-fn rs/as-unqualified-lower-maps})))

(defn update-aggregate
  [ctx agr]
  (log/debug "Updating aggregate" agr)
  (let [index (str/replace (:service-name ctx) "-" "_")]
    (elastic/query "POST"
                   (str "/" index "/_doc/" (:id agr))
                   (util/to-json agr))))

(defn flatten-paths
  ([m separator]
   (flatten-paths m separator []))
  ([m separator path]
   (->> (map (fn [[k v]]
               (if (and (map? v) (not-empty v))
                 (flatten-paths v separator (conj path k))
                 [(->> (conj path k)
                       (map name)
                       (clojure.string/join separator)
                       keyword) v]))
             m)
        (into {}))))

(defn- add-to-keyword [kw app-str]
  (keyword (str (name kw)
                app-str)))

(defn create-simple-query
  [query]
  (util/to-json
   {:size  600
    :query {:bool
            {:must (mapv
                    (fn [%]
                      {:term {(add-to-keyword (first %) ".keyword")
                              (second %)}})
                    (seq (flatten-paths query ".")))}}}))

(defn simple-search
  [ctx query]
  (log/debug "Executing simple search" query)
  (let [index (str/replace (get ctx :index-name
                                (:service-name ctx)) "-" "_")
        param (dissoc query :query-id)
        body (elastic/query
              "POST"
              (str "/" index "/_search")
              (create-simple-query param))]

    (mapv
     (fn [%]
       (get % :_source))
     (get-in
      body
      [:hits :hits]
      []))))

(defn with-transaction
  [ctx func]
  (jdbc/with-transaction
    [tx (:con ctx)]
    (try-to-data
     #(func
       (assoc ctx :con tx)))))
