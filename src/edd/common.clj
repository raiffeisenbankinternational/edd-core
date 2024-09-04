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

(defn get-by-id
  [ctx & [query]]
  (if-let [id (cond
                (:id query) (:id query)
                (:id ctx) (:id ctx)
                query (parse-param query)
                :else nil)]
    (let [aggregate (el-event/get-by-id ctx id)]
      (if query
        aggregate
        (assoc ctx
               :aggregate aggregate)))
    (log/warn "Id is nil")))

(defn fetch-by-id
  [ctx & [query]]
  {:pre [(or (:query ctx)
             query)]}
  (if-let [id (or (-> ctx :query :id)
                  (:id query))]
    (search/get-snapshot ctx id)
    (log/warn "Fetch-by-id -> Id is nil")))

(defn get-sequence-number-for-id
  [ctx & [query]]
  {:pre [(or (:id ctx)
             query)]}
  (let [id (if (:id query)
             (:id query)
             (parse-param query))]
    (dal/get-sequence-number-for-id
     (if query
       (assoc ctx :id  id)
       ctx))))

(defn get-id-for-sequence-number
  [ctx & [query]]
  {:pre [(or (:sequence ctx)
             sequence)]}
  (dal/get-id-for-sequence-number
   (if query
     (assoc ctx :id (if (:sequence query)
                      (:sequence query)
                      (parse-param query)))
     ctx)))

(defn get-aggregate-id-by-identity
  [ctx & [query]]
  {:pre [(or (:identity ctx)
             query)]}
  (let [identity (or (:identitiy ctx)
                     (parse-param query))
        resp (request-cache/get-identitiy ctx identity)]
    (if resp
      resp
      (dal/get-aggregate-id-by-identity
       (assoc ctx :identity identity)))))

(defn get-aggregate-by-identity
  [ctx & [query]]
  {:pre [(or (:identity ctx)
             query)]}
  (let [id (get-aggregate-id-by-identity query)]
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

(defn get-by-id-and-version-v2
  [ctx query]
  (let [{:keys [id ^int version]} query]
    (when (> version 0)
      (search/get-by-id-and-version ctx id version))))
