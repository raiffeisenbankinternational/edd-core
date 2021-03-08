(ns edd.common
  (:require
   [edd.dal :as dal]
   [edd.search :as search]
   [edd.cache :as cache]
   [lambda.uuid :as uuid]))

(defn get-by-id
  [ctx & [query]]
  {:pre [(or (:id ctx)
             (:id query))]}
  (cache/get-by-id ctx query))

(defn get-sequence-number-for-id
  [ctx & [id]]
  {:pre [(or (:id ctx)
             id)]}
  (dal/get-sequence-number-for-id
   (if id
     (assoc ctx :id id)
     ctx)))

(defn get-id-for-sequence-number
  [ctx & [sequence]]
  {:pre [(or (:sequence ctx)
             sequence)]}
  (dal/get-id-for-sequence-number
   (if sequence
     (assoc ctx :id sequence)
     ctx)))

(defn get-aggregate-id-by-identity
  [ctx & [identity]]
  {:pre [(or (:identity ctx)
             identity)]}
  (dal/get-aggregate-id-by-identity
   (if identity
     (assoc ctx :identity identity)
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
