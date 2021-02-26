(ns edd.common
  (:require
    [edd.el.event :as event]
    [edd.dal :as dal]
    [lambda.uuid :as uuid]
    [next.jdbc.result-set :as rs]))


(defn get-by-id
  [ctx & [query]]
  {:pre [(or (:id ctx)
             (:id query))]}
  (let [final-ctx (if query
                    (assoc ctx :id (:id query))
                    ctx)
        resp (event/get-by-id final-ctx)]
    (if query
      (:aggregate resp)
      resp)))


(defn create-identity
  [& _]
  (uuid/gen))
