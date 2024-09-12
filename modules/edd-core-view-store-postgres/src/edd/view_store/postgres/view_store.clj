(ns edd.view-store.postgres.view-store
  "
  PG-specific implementation of the view-store abstraction.
  "
  (:import
   clojure.lang.Keyword)
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.common
    :refer [->long!
            ->realm
            ->service
            flatten-paths]]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.parser :as parser]
   [edd.postgres.pool :refer [->conn]]
   [edd.s3.view-store :as s3.vs]
   [edd.search :refer [with-init
                       simple-search
                       advanced-search
                       update-aggregate
                       get-snapshot]]
   [lambda.util :as util]))

(defmethod with-init
  :postgres
  [ctx body-fn]
  (body-fn ctx))

(defmethod update-aggregate
  :postgres
  [ctx]

  (let [{:keys [aggregate]}
        ctx

        {:keys [id
                version]}
        aggregate

        realm
        (->realm ctx)

        service
        (->service ctx)

        conn
        (->conn ctx)]

    (api/upsert conn realm service aggregate)
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
            (parser/validate-simple-search!))

        conn
        (->conn ctx)]

    (api/find-by-attrs conn realm service attrs)))

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

        conn
        (->conn ctx)

        ;;
        ;; A workaround for pagination: internally bump
        ;; the limit value to get N+1 rows. Then decide
        ;; if we have more rows in the database and drop
        ;; the last row.
        ;;
        query-parsed-fix
        (assoc query-parsed :size [:integer (inc limit)])

        aggregates
        (api/find-advanced-parsed conn
                                  realm
                                  service
                                  query-parsed-fix)

        total
        (count aggregates)

        has-more?
        (> total limit)

        hits
        (if has-more?
          ;; subvec is O(1), no traverse
          (subvec aggregates 0 (dec total))
          aggregates)]

    {:total total
     :size limit
     :from offset
     :hits hits
     :has-more? has-more?}))

(defmethod get-snapshot
  :postgres
  [ctx id]
  (let [conn
        (->conn ctx)

        realm
        (->realm ctx)

        service
        (->service ctx)]

    (api/get-by-id conn realm service id)))

(defn register
  [ctx]
  (assoc ctx :view-store :postgres))
