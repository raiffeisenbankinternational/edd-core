(ns edd.common
  (:require
    [edd.el.event :as event]
    [edd.dal :as dal]
    [lambda.uuid :as uuid]
    [lambda.request :as request]
    [next.jdbc.result-set :as rs]))


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


(defn create-identity
  [& _]
  (uuid/gen))
