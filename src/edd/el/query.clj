(ns edd.el.query
  (:require [clojure.tools.logging :as log]
            [edd.schema :as s]
            [malli.core :as m]))

(defn handle-query
  [ctx body]
  (let [query (:query body)
        query-id (keyword (:query-id query))
        query-handler (query-id (:query ctx))
        schema (get-in ctx [:spec query-id]
                       [:map [:query-id keyword?]] )]

    (log/debug "Handling query" query)
    (log/debug "Query handler" query-handler)
    (if (m/validate schema query)
      (if query-handler
        (let [resp (query-handler ctx query)]
          (log/debug "Query response" resp)
          resp))
      {:error (s/explain-error schema query)})))
