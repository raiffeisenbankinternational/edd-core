(ns edd.view-store.postgres.schema
  "
  Malli schema and parser for Open Search DSL.
  "
  (:require
   [malli.core :as malli]))

(defn ->enum [& kws]
  (-> [:enum]
      (into kws)
      (into (map name kws))))

(def Attr
  [:or :keyword :string])

(def Value
  [:or
   :nil
   :int
   :double
   :string
   :symbol
   :keyword
   :boolean
   :uuid])

(def ValueColl
  [:or
   [:sequential {:min 1} Value]
   [:set {:min 1} Value]])

(def Sort
  (malli/schema
   [:schema
    {:registry

     {::Attr Attr

      ::SortOrder
      (->enum :asc
              :desc
              :asc-number
              :desc-number
              :asc-date
              :desc-date)

      ::SortPair
      [:catn
       [:attr ::Attr]
       [:order ::SortOrder]]

      ::Sort
      [:+ ::SortPair]}}

    ::Sort]))

(def SortParser
  (malli/parser Sort))

(def Search
  (malli/schema
   [:schema
    {:registry

     {::Attr Attr

      ::Attrs
      [:sequential {:min 1} [:ref ::Attr]]

      ::Search
      [:catn
       [:_1 [:= :fields]]
       [:attrs [:sequential {:min 1} [:ref ::Attr]]]
       [:_2 [:= :value]]
       [:value :string]]}}

    ::Search]))

(def Select
  (malli/schema
   [:schema
    {:registry
     {::Attr Attr

      ::Select
      [:sequential {:min 1} [:ref ::Attr]]}}

    ::Select]))

(def SelectParser
  (malli/parser Select))

(def Filter
  (malli/schema
   [:schema
    {:registry

     {::Op
      (->enum :eq := :==
              :lt :<
              :lte :<=
              :gte :>=
              :gt :>)
      ::Attr Attr
      ::Value Value
      ::ValueColl ValueColl

      ::Condition
      (->enum :and :or)

      ::PredicateExists
      [:catn
       [:op [:= :exists]]
       [:attr [:schema [:ref ::Attr]]]]

      ::PredicateIn
      [:catn
       [:op [:= :in]]
       [:attr [:schema [:ref ::Attr]]]
       [:value [:or
                [:sequential ::Value]
                [:set ::Value]]]]

      ;; A weird query that someone sends to facility service like:
      ;; [:wildcard "id" <UUID>]
      ;; We remap it into the `id = <UUID>` clause  which is much faster
      ;; as it hits PK index and doesn't affect the JSONb data.
      ;; TODO: spot the guilty service and fix it.
      ::PredicateIdUUID
      [:catn
       [:op (->enum :eq :wildcard :=)]
       [:attr (->enum :id)]
       [:value :uuid]]

      ::PredicateIdInUUID
      [:catn
       [:op (->enum :in)]
       [:attr (->enum :id)]
       [:value [:sequential {:min 1} :uuid]]]

      ::PredicateWildcard
      [:catn
       [:op [:= :wildcard]]
       [:attr [:schema [:ref ::Attr]]]
       ;; TODO: maybe limit to {:min 3} because of trigram index?
       ;; Because when len(term) < 3, it won't apply
       [:value :string]]

      ::PredicateSimple
      [:catn
       [:op [:schema [:ref ::Op]]]
       [:attr [:schema [:ref ::Attr]]]
       [:value [:schema [:ref ::Value]]]]

      ::PredicateAssetClassCode
      [:catn
       [:op (->enum :in)]
       [:attr (->enum :attrs.risk-on.asset-class.asset-class-code)]
       [:value [:sequential {:min 1} :int]]]

      ;; Another weird query sent to facility service by someone
      ;; like [:eq :attrs.status :active :pending]
      ;; TODO: spot the guilty service and fix there.
      ::PredicateStatusVariadic
      [:catn
       [:op [:= :eq]]
       [:attr (->enum :attrs.status)]
       [:value1 [:schema [:ref ::Value]]]
       [:value2 [:schema [:ref ::Value]]]]

      ::PredicateDatetimeLess
      [:catn
       [:op (->enum :< :<= :lt :lte)]
       [:attr (->enum :attrs.creation-time
                      :attrs.resolution-date
                      :creation-time)]
       [:value [:re #"^\d{4}-\d{2}-\d{2}$"]]]

      ::PredicateDatetimeMore
      [:catn
       [:op (->enum :> :>= :gt :gte)]
       [:attr (->enum :attrs.creation-time
                      :attrs.resolution-date
                      :creation-time)]
       [:value [:re #"^\d{4}-\d{2}-\d{2}$"]]]

      ::Predicate
      [:orn
       [:predicate-datetime-less [:ref ::PredicateDatetimeLess]]
       [:predicate-datetime-more [:ref ::PredicateDatetimeMore]]
       [:predicate-asset-class-code [:ref ::PredicateAssetClassCode]]
       [:predicate-in-uuid [:ref ::PredicateIdInUUID]]
       [:predicate-in [:ref ::PredicateIn]]
       [:predicate-exists [:ref ::PredicateExists]]
       [:predicate-id-uuid [:ref ::PredicateIdUUID]]
       [:predicate-simple [:ref ::PredicateSimple]]
       [:predicate-wc [:ref ::PredicateWildcard]]
       [:predicate-status-variadic [:ref ::PredicateStatusVariadic]]]

      ::Nested
      [:catn
       [:tag [:= :nested]]
       [:attr [:schema [:ref ::Attr]]]
       [:group [:schema
                [:catn
                 [:condition [:schema [:ref ::Condition]]]
                 [:children
                  [:+ [:schema
                       [:orn
                        [:predicate-simple [:ref ::PredicateSimple]]
                        [:predicate-wc [:ref ::PredicateWildcard]]]]]]]]]]

      ;; Another weird case when UI sends broken expresions
      ;; like [:and] or [:or] but nothing else. For now, cap-
      ;; ture these and resolve to FALSE on a SQL level.
      ::GroupBroken
      [:tuple [:ref ::Condition]]

      ::GroupVariadic
      [:catn
       [:condition [:schema [:ref ::Condition]]]
       [:children [:+ [:schema [:ref ::Query]]]]]

      ::GroupArray
      [:catn
       [:condition [:schema [:ref ::Condition]]]
       [:children [:sequential [:schema [:ref ::Query]]]]]

      ::Negation
      [:catn
       [:not [:= :not]]
       [:query [:schema [:ref ::Query]]]]

      ::Query
      [:orn
       [:predicate [:ref ::Predicate]]
       [:nested [:ref ::Nested]]
       [:group-variadic [:ref ::GroupVariadic]]
       [:group-array [:ref ::GroupArray]]
       [:group-broken [:ref ::GroupBroken]]
       [:negation [:ref ::Negation]]]}}

    [:ref ::Query]]))

(def FilterParser
  (malli/parser Filter))

(def NumberOrString
  [:orn
   [:integer [:int]]
   [:string [:re #"\d+"]]])

(def AdvancedSearch
  (malli/schema
   [:map
    [:filter {:optional true} Filter]
    [:search {:optional true} Search]
    [:select {:optional true} Select]
    [:sort {:optional true} Sort]
    [:from {:optional true} NumberOrString]
    [:size {:optional true} NumberOrString]]))

(def AdvancedSearchParser
  (malli/parser AdvancedSearch))

(def SimpleSearch
  (malli/schema
   [:schema
    {:registry

     {::Attr Attr
      ::Value Value
      ::ValueColl ValueColl

      ::ValueVector
      [:or
       [:sequential {:min 1} ::Value]
       [:set {:min 1} ::Value]]

      ::SimpleSearch
      [:map-of ::Attr [:or
                       ::Value
                       ::ValueColl
                       [:ref ::SimpleSearch]]]}}

    ::SimpleSearch]))
