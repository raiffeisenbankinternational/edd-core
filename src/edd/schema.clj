(ns edd.schema
  (:require [malli.core :as m]
            [malli.util :as mu]
            [malli.error :as me]
            [malli.transform :as mt]))

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

(def command-schema
  (m/schema
   [:map
    [:cmd-id keyword?]
    [:id  uuid?]]))

(def query-schema
  (m/schema
   [:map
    [:query-id keyword?]]))

(defn- merge-schema [& schemas]
  (reduce #(mu/merge %1 %2) schemas))

(defn merge-cmd-schema [schema]
  (merge-schema command-schema schema))

(defn merge-query-schema [schema]
  (merge-schema query-schema schema))

(defn explain-error [schema entity]
  (->> entity
       (m/explain schema)
       (me/humanize)))
