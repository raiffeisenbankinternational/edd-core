(ns edd.schema.core
  (:require
   [malli.core :as m]
   [malli.util :as mu]
   [malli.transform :as mt]))

(defonce transformers
  (mt/transformer
   mt/default-value-transformer
   mt/strip-extra-keys-transformer))

(defn decode
  ([schema entity]
   (decode schema entity transformers))
  ([schema entity transformers]
   (m/decode schema entity transformers)))

(def YYYY-MM-DD #"\d{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])")

(defn cmd
  ([cmd-id]
   (m/schema
    [:map
     [:cmd-id {:json-schema/type "string"} [:= cmd-id]]
     [:id uuid?]]))
  ([cmd-id attrs]
   (-> (m/schema
        [:map
         [:cmd-id {:json-schema/type "string"} [:= cmd-id]]
         [:id uuid?]])
       (mu/assoc :attrs attrs))))

(def str-date
  [:re {:error/message "Not a valid date. The format should be YYYY-MM-DD"
        :json-schema/type "string"
        :json-schema/format "date"}
   YYYY-MM-DD])

(def amount
  (m/schema
   [:map-of {:min 1}
    [:keyword {:gen/elements [:EUR]
               :json-schema/type "string"}]
    int?]))

(def percentage
  (m/schema
   [:and
    number?
    [:>= 0]
    [:<= 1]]))

(def non-empty-string
  [:re
   {:error/message "Non empty string expected."}
   #"\S+"])

(defn schema-keys [schema]
  (let [props (:children (mu/to-map-syntax schema))]
    (map (fn [[p]] p) props)))


(defn replace-merge
  "Replaces all keys which are schema relevant by m2 and leaves
  the non schema relevant attributes in m1"
  [schema m1 m2]

  (let [ks (schema-keys schema)
        m* (apply dissoc m1 ks)]
    (merge m* m2)))
