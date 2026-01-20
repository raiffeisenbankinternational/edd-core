(ns edd.common
  (:require
   [clojure.tools.logging :as log]
   [edd.dal :as dal]
   [edd.el.event :as el-event]
   [edd.request-cache :as request-cache]
   [edd.search :as search]
   [lambda.uuid :as uuid]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn contains-keys
  [m & expected-keys]
  (reduce
   (fn [_ item]
     (if
      (item m)
       true
       (reduced false)))
   true
   expected-keys))

(defn parse-param
  [query]
  (if (or (uuid? query)
          (string? query)
          (number? query))
    query
    (let [filter (dissoc query :query-id)
          keys (vec filter)]
      (when (> (count keys)
               1)
        (throw (ex-info "Unable to determine filter. Needs to have only 1 key next to :query-id"
                        {:filter-count (count keys)})))
      (-> keys
          (first)
          (second)))))

(defn get-by-id-and-version-v2
  [ctx query]
  (let [{:keys [id ^int version]} query]
    (when (> version 0)
      (search/get-by-id-and-version ctx id version))))

(defn -get-by-id-in-ctx
  "Returns context with a aggregate response in it"
  [ctx]
  (let [{:keys [id]}
        ctx

        aggregate
        (el-event/get-by-id ctx id)]
    (assoc ctx :aggregate aggregate)))

(defn -get-by-query
  "Returns aggregate"
  [ctx query]
  (let [id (parse-param query)]
    (el-event/get-by-id ctx id)))

(defn -get-by-id
  "Returns aggregate"
  [ctx query]
  (let [{:keys [id]} query]
    (el-event/get-by-id ctx id)))

(defn -query-as-id?
  [query]
  (some? (parse-param query)))

(defn get-by-id
  "
  Returns aggregate based on information provided in query and ctx.
  if query contains `:version` attribute the historical value is returned!

  !Caution!
  Service should take care about compatibility with previous (historical) aggregate
  structures to fulfill current client expectations!
  "
  [ctx & [query]]
  ;; dispatch based on query and ctx
  (cond
    ;; if id and version both available in query
    (contains-keys query :id :version)
    (get-by-id-and-version-v2 ctx query)

    ;; if only id available in query
    (contains-keys query :id)
    (-get-by-id ctx query)

    ;; if ctx contains id
    (contains-keys ctx :id)
    (-get-by-id-in-ctx ctx)

    ;; may query is an id?
    (-query-as-id? query)
    (-get-by-query ctx query)

    :else
    (log/warn "Id is nil")))

(defn get-by-id!
  "
  Returns aggregate based on information provided in query and ctx.
  Only latest version of aggregate!.
  "
  [ctx & [query]]
  (get-by-id ctx [(if (map? query) (dissoc query :id) query)]))

(defn fetch-by-id
  [ctx & [query]]
  {:pre [(or (:query ctx)
             query)]}
  (if-let [id (or (-> ctx :query :id)
                  (:id query))]
    (search/get-snapshot ctx id)
    (log/warn "Fetch-by-id -> Id is nil")))

(defn get-aggregate-id-by-identity
  [ctx & [query]]
  {:pre [(or (:identity ctx)
             query)]}
  (let [identity (or (:identity ctx)
                     (parse-param query))
        resp (request-cache/get-identity ctx identity)]
    (if resp
      resp
      (dal/get-aggregate-id-by-identity
       (assoc ctx :identity identity)))))

(defn get-aggregate-by-identity
  [ctx & [query]]
  {:pre [(or (:identity ctx)
             query)]}
  (let [id (get-aggregate-id-by-identity ctx query)]
    (when id
      (merge {:id id}
             (get-by-id ctx id)))))

(defn advanced-search
  [ctx & [query]]
  {:pre [(or (:query ctx)
             query)]}
  (search/advanced-search
   (if query
     (assoc ctx :query query)
     ctx)))

(defn simple-search
  [ctx & [query]]
  {:pre [(or (:query ctx)
             query)]}
  (search/simple-search
   (if query
     (assoc ctx :query query)
     ctx)))

(defn create-identity
  [& _]
  (uuid/gen))

(defn get-by-id-and-version
  [ctx query]
  (let [{:keys [id ^long version]} query]
    (when (> version 0)
      (let [events      (dal/get-events (assoc ctx :id id))
            upper-bound (count events)]
        (when (> version upper-bound)
          (throw (ex-info "Version does not exist" {:error events})))
        (el-event/create-aggregate {} (take version events) (:def-apply ctx))))))
