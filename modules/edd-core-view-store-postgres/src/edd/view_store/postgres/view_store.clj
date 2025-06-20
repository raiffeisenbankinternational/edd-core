(ns edd.view-store.postgres.view-store
  "
  PG-specific implementation of the view-store abstraction.
  "

  (:require
   [clojure.tools.logging :as log]
   [edd.postgres.history :as history]
   [edd.postgres.pool :as pool :refer [*DB*]]
   [edd.s3.view-store :as s3.vs]
   [edd.search :refer [with-init
                       simple-search
                       advanced-search
                       update-aggregate
                       get-snapshot
                       get-by-id-and-version]]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.common
    :refer [->long!
            ->realm
            ->service
            vbutlast
            flatten-paths]]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.parser :as parser]
   [lambda.util :as util]))

(defmethod with-init
  :postgres
  [ctx body-fn]
  (pool/with-init ctx body-fn))

(defmethod update-aggregate
  :postgres
  [ctx]

  (let [{:keys [aggregate]}
        ctx

        realm
        (->realm ctx)

        service
        (->service ctx)]

    (api/upsert *DB* realm service aggregate)
    (s3.vs/store-to-s3 ctx)))

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
  [ctx id]
  (let [realm
        (->realm ctx)

        service
        (->service ctx)]

    ;; Read from S3 first, and then Postgres because:
    ;; 1) it helps to reduce stress on the database;
    ;; 2) some aggregates might be missing in the database,
    ;; so querying them triggers their reconstruction from
    ;; source events.
    ;; make postgres as fallback for s3
    (util/d-time
     (format "fetching aggregate by id, service: %s, realm: %s, id: %s" service realm id)
     (or (s3.vs/get-from-s3 ctx id)
         (api/get-by-id *DB* realm service id)))))

(defmethod get-by-id-and-version
  :postgres
  [ctx id version]
  (when-let [history-entry
             (history/get-by-id-and-version ctx id version)]
    (:aggregate history-entry)))

(defn register
  [ctx]
  (assoc ctx :view-store :postgres))
