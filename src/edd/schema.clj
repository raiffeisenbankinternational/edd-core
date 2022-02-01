(ns edd.schema
  (:require [malli.core :as m]
            [malli.util :as mu]
            [malli.error :as me]
            [malli.transform :as mt]
            [edd.schema.core :as schema-core]))

(def YYYY-MM-DD #"\d{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])")

(def date? [:re YYYY-MM-DD])

(defonce transformers
  (mt/transformer
   mt/default-value-transformer
   mt/strip-extra-keys-transformer))

(defn decode
  ([schema entity]
   (decode schema entity transformers))
  ([schema entity transformers]
   (m/decode schema entity transformers)))

(defn- merge-schema [& schemas]
  (reduce #(mu/merge %1 %2) schemas))

(defn merge-cmd-schema [schema cmd-id]
  (merge-schema (schema-core/EddCoreCommand cmd-id) (or schema
                                                        [:map])))

(defn merge-query-consumes-schema [schema cmd-id]
  (merge-schema (schema-core/EddCoreQueryConsumes cmd-id) (or schema
                                                              [:map])))

(defn merge-query-produces-schema [schema]
  (merge-schema schema-core/EddCoreQueryProduces (if schema
                                                   [:map
                                                    [:result schema]]
                                                   [:map
                                                    [:result
                                                     [:map]]])))

(defn explain-error [schema entity]
  (->> entity
       (m/explain schema)
       (me/humanize)))


