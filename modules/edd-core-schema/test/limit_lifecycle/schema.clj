(ns limit-lifecycle.schema
  (:require [malli.core :as m]
            [malli.util :as mu]))

(def GetCurrentLimitStatusQuery
  (m/schema
    [:map
     [:query-id
      {:json-schema/type "string"}
      [:= :get-current-limit-status]]
     [:limit-id uuid?]]))

(def GetLimitLifecycleByIdQuery
  (m/schema
    [:map
     [:query-id
      {:json-schema/type "string"}
      [:= :get-limit-lifecycle-by-id]]
     [:limit-id uuid?]]))

(def Node
  (m/schema
    [:map
     [:limit-id uuid?]
     [:node-id uuid?]
     [:status
      {:json-schema/type "string"}
      [:enum "created" "applied" "approved" "warehoused" "rejected" "withdrawn" "expired" "revoked"]]
     [:parent-ids [:vector uuid?]]
     [:data {:optional true} [:map
                              [:risk-taker-id {:optional true} uuid?]
                              [:risk-on-id {:optional true} uuid?]
                              [:product-set-id {:optional true} uuid?]]]]))
(defn assoc-node
  [schema]
  (mu/assoc schema :node Node))

(def CreateLimitCmd
  (assoc-node
   (m/schema
       [:map
        [:cmd-id {:json-schema/type "string"} [:= :create-limit]]
        [:id uuid?]])))

(def ApplyLimitCmd
  (m/schema
    [:map
     [:cmd-id [:= :apply-limit]]
     [:id uuid?]
     [:node Node]]))

(def WarehouseLimitCmd
  (m/schema
    [:map
     [:cmd-id [:= :warehouse-limit]]
     [:id uuid?]
     [:node Node]]))

(def RejectLimitCmd
  (m/schema
    [:map
     [:cmd-id [:= :reject-limit]]
     [:id uuid?]
     [:node Node]]))

(def WithdrawLimitCmd
  (m/schema
    [:map
     [:cmd-id [:= :withdraw-limit]]
     [:id uuid?]
     [:node Node]]))

(def ApproveLimitCmd
  (m/schema
    [:map
     [:cmd-id [:= :approve-limit]]
     [:id uuid?]
     [:node Node]]))
