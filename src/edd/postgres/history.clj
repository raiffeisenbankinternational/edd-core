(ns edd.postgres.history
  (:require
   [edd.postgres.const :as c]
   [edd.postgres.honey :as honey]
   [edd.postgres.pool :refer [*DB*]]
   [lambda.util :as util]
   [edd.postgres.common
    :refer [->realm
            ->service
            ->ref-date]]
   [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)

(defn ->table
  "
  Return a HoneySQL structure that becomes 'db_schema.table_name'.
  "
  [schema]
  [[:. (keyword schema) c/HISTORY-TABLE]])

(defn get-by-id-and-version
  "
  Returns a history entry by aggregate id and version
  "
  [ctx aggregate-id version]

  (let [realm
        (->realm ctx)

        table
        (->table realm)

        service
        (->service ctx)

        service-name
        (if (keyword? service)
          (name service)
          service)

        sql-map
        {:select c/AGGREGATE_HISTORY_FIELDS
         :from [table]
         :where [:and
                 [:= :id aggregate-id]
                 [:= :version version]
                 [:= :service-name service-name]]}]

    (when-let [result (honey/execute-one *DB* sql-map)]
      (update result :aggregate util/to-edn))))

(defn -insert-entries
  "
  Inserts new history entries into db
  "
  [ctx aggregates]
  (let [realm
        (->realm ctx)

        table
        (->table realm)

        service
        (->service ctx)

        ref-date
        (->ref-date ctx)

        service-name
        (if (keyword? service)
          (name service)
          service)

        entries
        (for [aggregate aggregates]
          (let [{:keys [id version]} aggregate]
            {:id id
             :version version
             :service-name service-name
             :aggregate (util/to-json aggregate)
             :valid-from ref-date}))

        sql-map
        {:insert-into table :values (vec entries)}]

    (doseq [{:keys [id version]} entries]
      (log/infof "insert history entry id=%s, version=%d" id version))

    (honey/execute *DB* sql-map)))

(defn -invalidate-entries
  "
  Invalidates history entry by setting :valid-until field up to provided version
  "
  [ctx id version]
  (let [realm
        (->realm ctx)

        ref-date
        (->ref-date ctx)

        table
        (->table realm)

        service
        (->service ctx)

        service-name
        (if (keyword? service)
          (name service)
          service)

        sql-map
        {:update table
         :set {:valid-until ref-date}
         :where [:and
                 [:= :id id]
                 [:= :service-name service-name]
                 [:< :version version]
                 [:= :valid-until nil]]}]

    (log/infof "invalidate history up to id=%s, version=%d" id version)
    (honey/execute *DB* sql-map)))

(defn new-entries
  "
  Function inserts new history entries for each aggregate version into db
  and invalidate previous one if any
  "
  [ctx aggregates]
  (let [aggregates
        (sort-by :version aggregates)

        {:keys [id version]}
        (last aggregates)

        {:keys [service-configuration]}
        ctx

        {:keys [history]}
        service-configuration

        ;; by default history is turned on
        history
        (if (some? history) history :enabled)

        service
        (->service ctx)]

    (log/infof "historisation for service %s is %s" service history)

    (when (= :enabled history)
      (-insert-entries ctx aggregates)
      (-invalidate-entries ctx id version))))
