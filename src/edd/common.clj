(ns edd.common
  (:require
    [edd.el.event :as event]
    [edd.dal :as dal]
    [next.jdbc.result-set :as rs]))


(defn get-by-id
  [ctx]
  {:pre [(:id ctx)]}
  (->> ctx
       (dal/get-events)
       (assoc ctx :events)
       (event/get-current-state)))


