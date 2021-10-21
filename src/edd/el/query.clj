(ns edd.el.query
  (:require [clojure.tools.logging :as log]
            [malli.core :as m]
            [malli.error :as me]
            [lambda.util :as util]))

(defn handle-query
  [ctx body]
  (let [query (:query body)
        query-id (keyword (:query-id query))
        {:keys [consumes
                handler]} (get-in ctx [:edd-core :queries query-id])]

    (log/debug "Handling query" query-id)
    (when-not handler
      (throw (ex-info "No handler found"
                      {:error    "No handler found"
                       :query-id query-id})))
    (when-not (m/validate consumes query)
      (throw (ex-info "Invalid request"
                      {:error (-> (m/explain consumes query)
                                  (me/humanize))})))
    (let [resp (util/d-time
                (str "handling-query: " query-id)
                (handler ctx query))]
      (log/debug "Query response" resp)
      resp)))
