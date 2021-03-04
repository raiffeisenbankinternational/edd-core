(ns edd.common
  (:require
    [edd.el.event :as event]
    [edd.dal :as dal]
    [edd.search :as search]
    [lambda.uuid :as uuid]
    [lambda.request :as request]))


(defn get-by-id
  [ctx & [query]]
  {:pre [(or (:id ctx)
             (:id query))]}
  (let [final-ctx (if query
                    (assoc ctx :id (:id query))
                    ctx)
        resp (try
               (event/get-by-id final-ctx)
               (catch Exception e
                 (ex-data e)))
        aggregate (if query
                    (:aggregate resp)
                    resp)
        version (get aggregate :version 0)
        id (:id final-ctx)]
    (when (and (bound? #'request/*request*)
               version)
      (swap! request/*request*
             #(assoc-in % [:last-event-seq id] version)))
    aggregate))

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
