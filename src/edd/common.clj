(ns edd.common
  (:require
   [edd.dal :as dal]
   [edd.search :as search]
   [edd.cache :as cache]
   [lambda.uuid :as uuid]))

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
  (cond
    (:id query) (cache/get-by-id ctx query)
    (:id ctx) (cache/get-by-id ctx)
    query (cache/get-by-id ctx {:id (parse-param query)})
    :else nil))

(defn get-sequence-number-for-id
  [ctx & [query]]
  {:pre [(or (:id ctx)
             query)]}
  (dal/get-sequence-number-for-id
   (if query
     (assoc ctx :id (if (:id query)
                      (:id query)
                      (parse-param query)))
     ctx)))

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
  (dal/get-aggregate-id-by-identity
   (if query
     (assoc ctx :identity (parse-param query))
     ctx)))

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
