(ns edd.el.query
  (:require [clojure.tools.logging :as log]
            [edd.schema :as schema]
            [malli.core :as m]
            [lambda.util :as util]))

(defn- validate-response-schema-setting [ctx]
  (let [setting (or (:response-schema-validation ctx)
                    (:response-schema-validation (:meta ctx)))]
    (assert #{nil :log-on-error :throw-on-error} setting)
    setting))

(defn- validate-response-schema? [ctx]
  (contains? #{:log-on-error :throw-on-error}
             (validate-response-schema-setting ctx)))

(defn- maybe-validate-response [ctx produces response]
  (when (and response
             (validate-response-schema? ctx))
    (let [wrapped {:result response}]
      (when-not (m/validate produces wrapped)
        (let [error (schema/explain-error produces wrapped)]
          (condp = (validate-response-schema-setting ctx)
            :throw-on-error
            (throw (ex-info "Invalid response" {:error error}))
            :log-on-error
            (log/warnf "Invalid response %s" (pr-str {:error error}))))))))

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
