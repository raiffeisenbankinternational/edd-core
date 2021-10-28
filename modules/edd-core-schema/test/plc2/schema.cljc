(ns plc2.schema
  (:require [malli.core :as m]
            [malli.util :as mu]
            [glms-schema.core :as gs]
            [plc2.buckets :as bs]))


(def types
  (m/schema
   [:enum {:json-schema/type "string"}
    :lms-facility
    :facility-request
    :exposure-facility
    :drawing]))

(def Timebucket
  (m/schema
   [:map
    [:start-date gs/str-date]
    [:end-date gs/str-date]
    [:amount gs/amount]]))

(def ConsolidationPoint
  (m/schema
   [:map
    [:risk-on-id uuid?]
    [:risk-taker-id uuid?]
    [:product-set-id uuid?]]))

(def RefreshConsolidationPointsCmd
  (gs/cmd :refresh-consolidation-points
       ConsolidationPoint))

(def Timeline
  (m/schema
   [:sequential Timebucket]))


(def UpdateNodeAmtCmd
  (gs/cmd :update-node-amt
       (m/schema
        [:map
         [:amt-ccy Timebucket]])))

(def SetParentIdCmd
  (gs/cmd :set-parent-id
       (m/schema
        [:map [:parent-id [:or nil? uuid?]]])))

(def RemoveParentCmd
  (gs/cmd :remove-parent))

(def FxRates
  (m/schema
   [:map
    [:business-date string?]
    [:batch-number number?]
    [:attrs
     [:sequential
      [:map
       [:ccy-code string?]
       [:exch-rate string?]]]]]))

(def UpdateNodeFxRateCmd
  (gs/cmd :update-node-fx-rate
       (m/schema
        [:map
         [:fx-rates {:optional true} [:or nil? FxRates]]])))

(def RequestSampleGenerationCmd
  (gs/cmd :request-sample-generation
       (m/schema
        [:map
         [:type types]
         [:width number?]
         [:parent-id {:optional true} uuid?]])))

(def UpdateNodeSumsCmd
  (gs/cmd :update-node-sums
       (m/schema any?)))

(def UpdateAttrsPayload
  (m/schema [:map
             [:type {:optional true} types]
             [:status {:optional true} keyword?]
             [:application-id {:optional true} uuid?]
             [:until-further-notice {:optional true} boolean?]
             [:multi-borrower {:optional true} boolean?]]))

(def UpdateAttrsCmd
  (gs/cmd :update-attrs
       UpdateAttrsPayload))

(def UpdateSimNode
  (gs/cmd :update-sim-node
       (m/schema
        [:map
         [:node-id uuid?]
         [:type {:optional true :default :facility-request} [:enum :facility-request]]
         [:amt-ccy {:optional true} Timebucket]])))

(def AddSimReference
  (gs/cmd :add-sim-reference
       (m/schema
        [:map
         [:sim-node-id uuid?]])))

(def StartAmendmentCmd
  (gs/cmd :start-amendment
       (m/schema
        [:map
         [:facility-request-id uuid?]])))

(def StopAmendmentCmd
  (gs/cmd :stop-amendment))

(def LimitBucket
  (m/schema
   [:map
    [:bucket bs/BucketDefinition]
    [:amount gs/amount]]))

(def TimeBuckets
  (m/schema
   [:and
    [:sequential
     LimitBucket]
    [:fn (fn [buckets]
           (->> buckets
                (map :bucket)
                (apply distinct?)))]]))

(def TopDownLimit
  (m/schema
   [:map
    [:risk-taker-id :uuid]
    [:risk-on-id :uuid]
    [:product-set-id :uuid]
    [:limit-category [:enum {:json-schema/type "string"} :cap :limit]]
    [:risk-type [:enum {:json-schema/type "string"} :gross :net :risk-weighted]]
    [:time-buckets TimeBuckets]]))

(def UpdateTopDownLimitCmd
  (gs/cmd :update-top-down-limit
       TopDownLimit))

(def UpdateTopDownLimitRequestCmd
  (gs/cmd :update-top-down-limit-request
       (mu/merge
        TopDownLimit
        (m/schema
         [:map
          [:status [:enum {:json-schema/type "string"} :created :applied :approved]]
          [:application-id :uuid]]))))

(def commands
  {:glms-plc2-svc/update-node-amt UpdateNodeAmtCmd
   :glms-plc2-svc/update-node-fx-rate UpdateNodeFxRateCmd})
