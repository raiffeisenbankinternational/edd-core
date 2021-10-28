(ns edd.schema.swagger
  (:require [malli.core :as m]
            [jsonista.core :as json]
            [clojure.tools.logging :as log]
            [malli.swagger :as swagger]))

(def ^:private template
  {:openapi    "3.0.3"
   :host       nil
   :basePath   nil
   :schemes    ["http" "https"]
   :components {:schemas {:commands nil
                          :queries  nil}}
   :paths
               {"/command"
                {:post
                 {:summary     ""
                  :description ""
                  :consumes    ["application/json"]
                  :produces    ["application/json"]
                  :parameters  nil
                  :responses   {200 {:description "OK" :type "object"}}}}
                "/query"
                {:post
                 {:summary     ""
                  :description ""
                  :consumes    ["application/json"]
                  :produces    ["application/json"]
                  :parameters  nil
                  :responses   {200 {:description "OK" :type "object"}}}}}})

(defn- read-schema
  "Return a map of schema names to their definitions defined in a
  `schema-ns`"
  [ctx]
  {:queries  (get-in ctx [:edd-core :queries :spec])
   :commands (:spec ctx)})






(defn- generate-substitutions [definitions]
  (let [transform (fn [m] (reduce-kv
                            (fn [acc k v]
                              (assoc acc k (swagger/transform v)))
                            {} m))]
    {:queries  {:queries (transform (:queries definitions))
                :parameters
                         [{:name     "query"
                           :in       "body"
                           :required true
                           :schema   {:oneOf (for [query (-> definitions :queries keys)]
                                               {:$ref (str "#/components/schemas/" (name query))})}}]}
     :commands {:commands (transform (:commands definitions))
                :parameters
                          [{:name     "command"
                            :in       "body"
                            :required true
                            :schema   {:oneOf (for [command (-> definitions :commands keys)]
                                                {:$ref (str "#/components/schemas/" (name command))})}}]}}))

(defn- generate-swagger
  [substitutions template]
  (-> template
      (assoc :basePath (:base-path substitutions))
      (assoc :host (:host substitutions))
      (assoc :info (:info substitutions))

      (assoc-in [:components :schemas] (-> substitutions :commands :commands))
      (update-in [:components :schemas] merge (-> substitutions :queries :queries))

      (assoc-in [:paths "/command" :post :parameters] (-> substitutions :commands :parameters))
      (assoc-in [:paths "/query" :post :parameters] (-> substitutions :queries :parameters))))



(defn generate
  [ctx {:keys [service hostname port title description version] :as _options}]
  (-> (read-schema ctx)
      (generate-substitutions)
      (merge {:host      (if port
                           (str hostname ":" port)
                           hostname)
              :base-path service
              :info      {:title       title
                          :description description
                          :version     version}})
      (generate-swagger template)))

(defn swagger-runtime
  [ctx & [_]]
  (log/info "Started swagger runtime")
  (let [result (->> (generate
                      ctx
                      {:service  (System/getenv "PROJECT_NAME")
                       :hostname "plc2-svc.lime.internal.rbigroup.cloud"})
                    (json/write-value-as-string))]
    (if-let [output (System/getProperty "edd.schema.out")]
      (spit output result)
      (print result))))


