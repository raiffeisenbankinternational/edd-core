(ns edd.view-store.postgres.history
  "History table operations for versioned aggregate snapshots.
   Stores each version of an aggregate for historical retrieval.
   
   Uses the same schema pattern as the main view store: {realm}_{service}.
   Table: aggregates_history with columns: id, version, data (JSONB), valid_from"
  (:require
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.jdbc :refer [as-aggregates]]))

(set! *warn-on-reflection* true)

(def ^:const HISTORY-TABLE :aggregates_history)

(defn ->history-table
  "Return a HoneySQL structure for the history table in the given schema."
  [realm service]
  [[:. (api/->schema realm service) HISTORY-TABLE]])

(defn insert-history-entry
  "Insert a new history entry for the given aggregate.
   Uses INSERT ... ON CONFLICT DO NOTHING to handle duplicate versions gracefully."
  [db realm service aggregate]
  (let [table (->history-table realm service)
        {:keys [id version]} aggregate
        sql-map {:insert-into table
                 :values [{:id id
                           :version (or version 0)
                           :data [:lift aggregate]}]
                 :on-conflict [:id :version]
                 :do-nothing true}]
    (honey/execute db sql-map)))

(defn get-by-id-and-version
  "Retrieve a specific version of an aggregate from the history table.
   Returns nil if the version doesn't exist."
  [db realm service aggregate-id version]
  (let [table (->history-table realm service)
        sql-map {:select [[:data :aggregate]]
                 :from [table]
                 :where [:and
                         [:= :id [:inline aggregate-id]]
                         [:= :version version]]}]
    (honey/execute-one db sql-map {:builder-fn as-aggregates})))
