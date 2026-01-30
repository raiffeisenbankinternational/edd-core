(ns edd.view-store.postgres.view-store
  "PostgreSQL view store multimethod implementations for EDD-Core.
   
   PREREQUISITES:
   - PostgreSQL database connection pool in context (via edd.postgres.pool)
   - AWS S3 credentials for backup storage
   - Database schema per realm-service (created via migrations)
   
   ARCHITECTURE:
   - Primary storage: PostgreSQL (JSONB for fast queries)
   - Backup storage: S3 (for disaster recovery and reduced DB load)
   - Read path: S3 first (fast), then Postgres (may trigger event replay)
   - Write path: Postgres + S3 in parallel
   
   MULTIMETHOD IMPLEMENTATIONS:
   - update-aggregate: Stores to Postgres, backs up to S3
   - get-snapshot: Reads from S3 cache, falls back to Postgres
   - get-by-id-and-version: Retrieves from event store history table
   - simple-search: Direct JSONB attribute matching
   - advanced-search: Advanced Search DSL queries parsed to SQL (supports OpenSearch and PostgreSQL)
   
   USAGE:
   (-> ctx (postgres-view-store/register))"

  (:require
   [clojure.tools.logging :as log]
   [edd.postgres.history :as es-history]
   [edd.postgres.pool :as pool :refer [*DB*]]
   [edd.s3.view-store :as s3.vs]
   [edd.search :refer [with-init
                       simple-search
                       advanced-search
                       update-aggregate
                       get-snapshot
                       get-by-id-and-version]]
   [edd.search.validation :as validation]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.common
    :refer [->realm
            ->service
            vbutlast]]
   [edd.view-store.postgres.history :as vs-history]
   [edd.view-store.postgres.parser :as parser]
   [lambda.util :as util]))

(defmethod with-init
  :postgres
  [ctx body-fn]
  (pool/with-init ctx body-fn))

(defmethod update-aggregate
  :postgres
  [ctx aggregate]
  (validation/validate-aggregate! ctx aggregate)
  (let [{:keys [service-configuration]} ctx
        realm (->realm ctx)
        service (->service ctx)
        ;; Check if history is enabled (default is enabled)
        history-setting (get service-configuration :history :enabled)]

    ;; Store to main aggregates table (latest version)
    (api/upsert *DB* realm service aggregate)
    ;; Write to view store's local history table (for standalone usage/compliance tests)
    ;; but only if historisation is enabled
    ;; Event store history is managed separately during store-results phase
    (when (= :enabled history-setting)
      (vs-history/insert-history-entry *DB* realm service aggregate))
    ;; Backup to S3
    (s3.vs/store-to-s3 ctx aggregate)
    ctx))

(defmethod simple-search
  :postgres
  [{:as ctx :keys [query]}]

  (log/infof "__PG SIMPLE SEARCH: %s" query)

  (let [realm
        (->realm ctx)

        service
        (->service ctx)

        attrs
        (-> query
            (dissoc :query-id)
            (parser/validate-simple-search!))]

    (api/find-by-attrs *DB* realm service attrs)))

(defmethod advanced-search
  :postgres
  [{:as ctx :keys [query]}]

  (log/infof "__PG ADVANCED SEARCH: %s" query)

  (let [query-parsed
        (parser/parse-advanced-search! query)

        {from-parsed :from
         size-parsed :size}
        query-parsed

        limit
        (parser/size-parsed->limit size-parsed)

        offset
        (parser/from-parsed->offset from-parsed)

        realm
        (->realm ctx)

        service
        (->service ctx)

        ;;
        ;; A workaround for pagination: internally bump
        ;; the limit value to get N+1 rows. Then decide
        ;; if we have more rows in the database and drop
        ;; the last row.
        ;;
        query-parsed-fix
        (assoc query-parsed :size [:integer (inc limit)])

        aggregates
        (api/find-advanced-parsed *DB*
                                  realm
                                  service
                                  query-parsed-fix)

        amount
        (count aggregates)

        has-more?
        (> amount limit)

        hits
        (if has-more?
          (vbutlast aggregates)
          aggregates)

        total
        (+ offset amount)]

    {:total total
     :size (count hits)
     :from offset
     :hits hits
     :has-more? has-more?}))

(defmethod get-snapshot
  :postgres
  [ctx id-or-query]
  (let [{:keys [id version]} (validation/validate-snapshot-query! ctx id-or-query)
        realm (->realm ctx)
        service (->service ctx)]

    (if version
      ;; Specific version requested - S3 history primary, local view-store history fallback
      (util/d-time
       (format "PostgresViewStore fetching aggregate by id and version, service: %s, realm: %s, id: %s, version: %s"
               service realm id version)
       (or
        ;; Primary: S3 history (versioned snapshots)
        (s3.vs/get-history-from-s3 ctx id version)
        ;; Fallback: View store's local history (for standalone usage/compliance tests)
        ;; Returns the aggregate directly (no wrapper map) due to AggregateBuilder
        (vs-history/get-by-id-and-version *DB* realm service id version)))
      ;; Latest version - read S3 first (cache), then Postgres (may trigger event replay)
      (util/d-time
       (format "PostgresViewStore fetching aggregate by id, service: %s, realm: %s, id: %s" service realm id)
       (or (util/d-time
            "PostgresViewStore Fetching from S3"
            (s3.vs/get-from-s3 ctx id))
           (util/d-time
            "PostgresViewStore Fetching from database"
            (api/get-by-id *DB* realm service id)))))))

(defmethod get-by-id-and-version
  :postgres
  [ctx id version]
  (validation/validate-id-and-version! ctx id version)
  (let [realm (->realm ctx)
        service (->service ctx)]
    (or
     ;; Primary: Event store history (populated during full command/apply flow)
     ;; Returns {:aggregate <data>} so we extract :aggregate
     (when-let [history-entry (es-history/get-by-id-and-version ctx id version)]
       (:aggregate history-entry))
     ;; Fallback: View store's local history (for standalone usage/compliance tests)
     ;; Returns the aggregate directly (no wrapper map) due to AggregateBuilder
     (vs-history/get-by-id-and-version *DB* realm service id version))))

(defn register
  [ctx]
  (assoc ctx :view-store :postgres))
