(ns lambda.service-schema
  (:require [jsonista.core :as json]
            [yaml.core :as yaml]))

(def ^:private schema-format->serializer-fn
  {"json" json/write-value-as-string ;; NOTE we can't use lambda.util/to-json since its encoder interferes with JSON Schema references (eg. encodes "#/path" -> "##/path")
   "yaml" yaml/generate-string})

(def ^:private schema-format->content-type
  {"json" "application/json"
   "yaml" "application/yaml"})

(defn handler [ctx]
  (let [service-schema (get-in ctx [:edd-core :service-schema])
        schema-format (-> ctx :service-schema-request :format)]
    (assoc ctx
           :resp service-schema
           :resp-serializer-fn (schema-format->serializer-fn schema-format)
           :resp-content-type (schema-format->content-type schema-format))))

(def ^:private request-path-re #"/api/schema\.(json|yaml)")

(defn relevant-request? [http-method path]
  (and (= http-method "GET")
       (re-matches request-path-re path)))

(defn requested-format [path]
  (->> path
       (re-matches request-path-re)
       (second)))
