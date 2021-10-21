(ns edd.schema.swagger
  (:gen-class)
  (:require [malli.core :as m]
            [malli.util :as mu]
            [yaml.core :as yaml]
            [clojure.tools.logging :as log]
            [malli.swagger :as swagger]
            [edd.schema.core :as schema-core]
            [yaml.writer :refer [YAMLWriter]]
            [jsonista.core :as json]))

(def ^:private template
  {:openapi    "3.0.3"
   :components {:schemas {:command-success (swagger/transform schema-core/EddCoreCommandSuccess)
                          :command-error   (swagger/transform schema-core/EddCoreCommandError)
                          :query-error     (swagger/transform schema-core/EddCoreCommandError)}}
   :paths      {}})

(defn- read-schema
  "Return a map of schema names to their definitions defined in a
  `schema-ns`"
  [ctx]
  {:queries  (get-in ctx [:edd-core :queries])
   :commands (get-in ctx [:edd-core :commands])})

(defn cmd-schemas->swagger
  [m]
  (reduce-kv
    (fn [acc k {:keys [consumes]}]
      (let [full-schema [:map
                         [:command consumes]]]
        (assoc acc k
                   (swagger/transform
                     (mu/merge
                       schema-core/EddCoreRequest
                       (mu/merge (schema-core/EddCoreSingleCommandRequest k)
                                 full-schema))))))
    {} m))

(defn query-schemas->swagger
  [m]
  (reduce-kv
    (fn [acc k {:keys [produces consumes]}]
      (assoc acc (str (name k) "-consumes") (swagger/transform
                                              (mu/merge consumes
                                                        schema-core/EddCoreRequest))
                 (str (name k) "-produces") (swagger/transform
                                              (mu/merge produces
                                                        schema-core/EddCoreResponse))))
    {}
    m))

(defn cmd->swagger-path
  [_ cmd]
  {:post
   {:summary     ""
    :description ""
    :requestBody {:required true
                  :content  {
                             :application/json
                             {:schema
                              {:$ref (str "#/components/schemas/" (name cmd))}}}}

    :responses   {"200" {:description "OK"
                         :content     {"application/json"
                                       {:schema
                                        {:$ref (str "#/components/schemas/command-success")}}}}
                  "501" {:description "OK"
                         :content     {"application/json"
                                       {:schema
                                        {:$ref (str "#/components/schemas/command-error")}}}}}}})

(defn query->swagger-path
  [_ query]
  {:post
   {:summary     ""
    :description ""
    :requestBody {:required true
                  :content  {
                             :application/json
                             {:schema
                              {:$ref (str "#/components/schemas/" (name query) "-consumes")}}}}
    :responses   {"200" {:description "OK"
                         :content     {"application/json"
                                       {:schema
                                        {:$ref (str "#/components/schemas/"
                                                    (name query)
                                                    "-produces")}}}}
                  "501" {:description "OK"
                         :content     {"application/json"
                                       {:schema
                                        {:$ref (str "#/components/schemas/query-error")}}}}}}})

(defn- generate-swagger
  [definitions template]

  (-> template
      (update-in [:components :schemas]
                 merge (cmd-schemas->swagger (:commands definitions)))
      (update-in [:components :schemas]
                 merge (query-schemas->swagger (:queries definitions)))

      (update-in [:paths]
                 (fn [paths]
                   (reduce
                     (fn [s cmd]
                       (assoc s (str "/command/" (name cmd))
                                (cmd->swagger-path definitions cmd)))
                     paths
                     (-> definitions :commands keys))))
      (update-in [:paths]
                 (fn [paths]
                   (reduce
                     (fn [s query]
                       (assoc s (str "/query/" (name query))
                                (query->swagger-path definitions query)))
                     paths
                     (-> definitions :queries keys))))))



(defn generate
  [ctx {:keys [service] :as _options}]
  (let [schema (read-schema ctx)]
    (merge {:info {:title   (or service "api")
                   :version "1.0"}}
           (generate-swagger schema template))))

(extend-protocol YAMLWriter
  java.util.regex.Pattern
  (encode [data] (str data)))

(defn swagger-runtime
  [ctx & [_]]
  (log/info "Started swagger runtime")
  (let [result (->> (generate
                      ctx
                      {:service (System/getenv "PROJECT_NAME")}))
        result (if (= (:edd/schema-format ctx) "json")
                 (json/write-value-as-string result)
                 (yaml/generate-string result))]
    (if-let [output (or (:edd/schema-out ctx)
                        (System/getProperty "edd.schema.out"))]
      (spit output result)
      (print result))))




