(ns edd.schema.core
  (:require
   [malli.core :as m]
   [malli.util :as mu]
   [malli.transform :as mt]))

#?(:clj
   (set! *warn-on-reflection* true))

#?(:clj
   (set! *unchecked-math* :warn-on-boxed))

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

(def date? [:re YYYY-MM-DD])

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
  [:re {:error/message      "Not a valid date. The format should be YYYY-MM-DD"
        :json-schema/type   "string"
        :json-schema/format "date"}
   YYYY-MM-DD])

(def amount
  (m/schema
   [:map-of {:min 1}
    [:keyword {:gen/elements     [:EUR]
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
  (mapv first (m/children schema)))

(defn replace-merge
  "Replaces all keys which are schema relevant by m2 and leaves
  the non schema relevant attributes in m1"
  [schema m1 m2]

  (let [ks (schema-keys schema)
        m* (apply dissoc m1 ks)]
    (merge m* m2)))

(def request-id-description
  "Represents one invocation from client. Must be unique for every invocation.
  I is used for de-duplication and if same request-id is used service
  will ignore request")

(def interraction-id-description
  "Represents usually one user session which has multiple request.
  Does not need ot be re-used and does not need to be unique")

(def EddCoreRequest
  [:map
   [:request-id {:description request-id-description}
    :uuid]
   [:interaction-id {:description interraction-id-description}
    :uuid]])

(def EddCoreResponse
  [:map
   [:invocation-id {:description "Invocation ID represents backend invocation
                                  id for this execution."} :uuid]
   [:request-id {:description request-id-description}
    :uuid]
   [:interaction-id {:description interraction-id-description}
    :uuid]])

(defn EddKeyword
  ([] [:fn {:json-schema/type "string"} keyword?])
  ([k] [:fn {:json-schema/type "string"
             :json-schema/enum [(str k)]} #(= (keyword %) k)]))

(defn EddCoreCommand
  ([] [:map
       [:id {:description "Id of aggregate that command is mutating"}
        :uuid]
       [:cmd-id {:description "Selects which command will be executed on backend"}
        :keyword]])
  ([cmd-id] [:map
             [:id :uuid]
             [:cmd-id (EddKeyword cmd-id)]]))

(def EddCoreCommandRequest
  [:map
   [:commands [:vector (EddCoreCommand)]]])

(defn EddCoreSingleCommandRequest
  ([] [:map
       [:command (EddCoreCommand)]])
  ([cmd-id] [:map
             [:command (EddCoreCommand cmd-id)]]))

(def EddCoreCommandSuccess
  [:map
   [:result
    [:map
     [:success {:description "Indicates if response was successfull"}
      :boolean]
     [:effects {:description "Indicates how many asnc actions where triggers"}
      :int]
     [:events {:description "NUmber of events produced"}
      :int]
     [:identities {:description "Number of identities created"}
      :int]]]
   [:invocation-id {:description "Invocation ID represents backend invocation
                                  id for this execution."} :uuid]
   [:request-id {:description request-id-description}
    :uuid]
   [:interaction-id {:description interraction-id-description}
    :uuid]])

(def EddCoreCommandError
  [:map
   [:errors
    [:vector [:map
              [:message string?]]]]
   [:invocation-id {:description "Invocation ID represents backend invocation
                                  id for this execution."} :uuid]
   [:request-id {:description request-id-description}
    :uuid]
   [:interaction-id {:description interraction-id-description}
    :uuid]])

(defn EddCoreQueryConsumes
  ([] [:map
       [:query-id keyword?]])
  ([query-id] [:map
               [:query-id (EddKeyword query-id)]]))

(def EddCoreQueryProduces
  [:map
   [:result [:map]]])
