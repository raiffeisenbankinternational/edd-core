(ns edd.ctx
  (:require [clojure.tools.logging :as log]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def EddCoreAggregateSchema
  [:map])

(def EddCoreRegCmd
  (m/schema
   [:map
    [:handler [:fn fn?]]
    [:id-fn [:fn fn?]]
    [:deps [:or
            [:map]
            [:vector :any]]]
    [:consumes
     [:fn #(m/schema? (m/schema %))]]]))

(def EddCoreRegQuery
  (m/schema
   [:map
    [:handler [:fn fn?]]
    [:deps [:or
            [:map]
            [:vector :any]]]
    [:produces
     [:fn #(m/schema? (m/schema %))]]
    [:consumes
     [:fn #(m/schema? (m/schema %))]]]))

(defn get-service-schema
  [ctx]
  (get-in ctx [:edd-core :service-schema]
          EddCoreAggregateSchema))

(defn put-service-schema
  [ctx schema]
  (assoc-in ctx [:edd-core :service-schema] (mu/merge
                                             EddCoreAggregateSchema
                                             schema)))

(defn get-cmd
  [ctx cmd-id]
  (get-in ctx [:edd-core :commands cmd-id]))

(defn put-cmd
  [ctx & {:keys [cmd-id
                 options]}]
  (assoc-in ctx [:edd-core :commands cmd-id] options))

(defn get-query
  [ctx query-id]
  (get-in ctx [:edd-core :queries query-id]))

(defn get-features
  [ctx]
  (get-in ctx [:edd-core :features] []))

(defn put-feature
  [ctx feature]
  (let [feature-id
        (:feature-id feature)]
    (when (some
           (fn [existing]
             (= (:feature-id existing) feature-id))
           (get-features ctx))
      (throw (ex-info "Feature already registered"
                      {:feature feature-id})))
    (update-in ctx [:edd-core :features] (fnil conj []) feature)))

(defn deps-keys
  [deps]
  (if (vector? deps)
    (map first (partition 2 deps))
    (keys deps)))

(defn run-filters
  "Runs the feature filters registered under hook-key as a servlet-style
  chain around terminal. Each filter is (fn [ctx request chain]) and is
  responsible for calling (chain ctx request) to proceed - not calling it
  short-circuits with the filter's own return value. The first registered
  feature is the outermost filter; terminal is (fn [ctx request])."
  [ctx hook-key terminal request]
  (let [filters
        (keep hook-key (get-features ctx))

        chain
        (reduce
         (fn [next-link filter-fn]
           (fn [ctx request]
             (filter-fn ctx request next-link)))
         terminal
         (reverse filters))]
    (chain ctx request)))

(defn- schema-keys
  [schema]
  (map first (m/entries (m/schema schema))))

(def ^:private core-registration-keys
  (set (concat (schema-keys EddCoreRegCmd)
               (schema-keys EddCoreRegQuery))))

(defn- report-and-throw
  [message data-key items]
  (when (seq items)
    (doseq [item items]
      (log/errorf "%s: %s" message (pr-str item)))
    (throw (ex-info message
                    {data-key (vec items)}))))

(defn- feature-pair-conflicts
  "Conflicting keys between every pair of features, keys taken from
  each feature with key-fn."
  [features key-fn]
  (for [[feature & later] (take-while seq (iterate rest features))
        :let [feature-keys
              (set (key-fn feature))]
        other later
        :let [conflicting
              (filter feature-keys (key-fn other))]
        :when (seq conflicting)]
    {:feature (:feature-id other)
     :conflicts-with {:feature (:feature-id feature)}
     :conflicting-keys (vec conflicting)}))

(defn- validate-feature-schema-overlaps
  [features]
  (let [feature-schema-keys
        (fn [{:keys [schema]}]
          (when schema
            (schema-keys schema)))

        against-core
        (for [feature features
              :let [overlapping
                    (filter core-registration-keys (feature-schema-keys feature))]
              :when (seq overlapping)]
          {:feature (:feature-id feature)
           :conflicts-with :edd-core
           :conflicting-keys (vec overlapping)})

        against-features
        (feature-pair-conflicts features feature-schema-keys)]
    (report-and-throw "Feature schema overlaps"
                      :overlaps
                      (concat against-core against-features))))

(defn- validate-feature-deps-conflicts
  [ctx]
  (let [features
        (get-features ctx)

        against-features
        (feature-pair-conflicts features
                                (fn [feature]
                                  (deps-keys (:deps feature))))

        against-registrations
        (for [{:keys [feature-id deps]} features
              :let [feature-keys
                    (set (deps-keys deps))]
              registry [:commands :queries]
              [id options] (get-in ctx [:edd-core registry])
              :let [conflicting
                    (filter feature-keys (deps-keys (:deps options)))]
              :when (seq conflicting)]
          {:feature feature-id
           :conflicts-with {registry id}
           :conflicting-keys (vec conflicting)})]
    (report-and-throw "Feature deps conflicts"
                      :conflicts
                      (concat against-features against-registrations))))

(defn- strict-registration-schema
  "Base registration schema merged with every feature :schema (feature
  keys made optional), root closed so unknown option keys are rejected."
  [base features]
  (let [merged
        (reduce
         (fn [schema feature-schema]
           (mu/merge schema (mu/optional-keys (m/schema feature-schema))))
         base
         (keep :schema features))]
    (mu/update-properties merged assoc :closed true)))

(defn- validate-registrations
  [ctx]
  (let [features
        (get-features ctx)

        errors
        (for [[registry base] {:commands EddCoreRegCmd
                               :queries EddCoreRegQuery}
              :let [schema
                    (strict-registration-schema base features)]
              [id options] (get-in ctx [:edd-core registry])
              :when (not (m/validate schema options))]
          {registry id
           :explain (me/humanize (m/explain schema options))})]
    (report-and-throw "Invalid registrations"
                      :errors
                      errors)))

(defn init-features
  "Validates feature :schema overlaps, deps conflicts and every
  registration against the merged closed schema, then runs every
  feature :init hook over the fully-assembled ctx.
  Must be called after all registrations. Idempotent."
  [ctx]
  (if (get-in ctx [:edd-core :features-initialized])
    ctx
    (let [_
          (validate-feature-schema-overlaps (get-features ctx))

          _
          (validate-feature-deps-conflicts ctx)

          _
          (validate-registrations ctx)

          ctx
          (reduce
           (fn [ctx {:keys [init]}]
             (if init
               (init ctx)
               ctx))
           ctx
           (get-features ctx))]
      (assoc-in ctx [:edd-core :features-initialized] true))))
