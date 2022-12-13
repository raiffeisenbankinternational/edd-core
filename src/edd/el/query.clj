(ns edd.el.query
  (:require [clojure.tools.logging :as log]
            [edd.schema :as schema]
            [malli.core :as m]
            [lambda.util :as util]))

(defn- maybe-validate-response [ctx produces response]
  (when (and response
             (:validate-response-schema? ctx))
    (let [wrapped {:result response}]
      (when-not (m/validate produces wrapped)
        (throw (ex-info "Invalid response"
                        {:error (schema/explain-error produces wrapped)}))))))

(defn handle-query
  [ctx body]
  (let [query (:query body)
        query-id (keyword (:query-id query))
        {:keys [consumes
                handler
                produces]} (get-in ctx [:edd-core :queries query-id])]

    (log/debug "Handling query" query-id)
    (when-not handler
      (throw (ex-info "No handler found"
                      {:error    "No handler found"
                       :query-id query-id})))
    (when-not (m/validate consumes query)
      (throw (ex-info "Invalid request"
                      {:error (schema/explain-error consumes query)})))
    (let [resp (util/d-time
                (str "handling-query: " query-id)
                (handler ctx query))]
      (log/debug "Query response" resp)
      (maybe-validate-response ctx produces resp)
      resp)))
