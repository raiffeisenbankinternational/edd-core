(ns edd.ctx
  (:require [malli.util :as mu]))

(def EddCoreAggregateSchema
  [:map])

(defn get-service-schema
  [ctx]
  (get-in ctx [:edd-core :service-schema]
          EddCoreAggregateSchema))

(defn put-service-schema
  [ctx schema]
  (assoc-in ctx [:edd-core :service-schema] (mu/merge
                                             EddCoreAggregateSchema
                                             schema)))

(defn get-cmd
  [ctx cmd-id]
  (get-in ctx [:edd-core :commands cmd-id]))

(defn put-cmd
  [ctx & {:keys [cmd-id
                 options]}]
  (assoc-in ctx [:edd-core :commands cmd-id] options))

(defn get-query
  [ctx query-id]
  (get-in ctx [:edd-core :queries query-id]))
