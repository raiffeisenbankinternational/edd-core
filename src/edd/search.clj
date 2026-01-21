;; View Store (Aggregate/Snapshot Store) Multimethods
;;
;; Defines the interface for materialized view operations in CQRS architecture.
;; Implementations dispatch on :view-store key in context.
;;
;; REALM SUPPORT:
;; All implementations MUST support multi-tenancy via realm isolation.
;; Realm is extracted from ctx [:meta :realm], defaults to :test.
;; All queries and updates are scoped to the current realm.
;;
;; AVAILABLE IMPLEMENTATIONS:
;; - :memory (edd.memory.view-store) - In-memory, realm-partitioned, for testing
;; - :elastic (edd.elastic.*) - Elasticsearch with realm filtering
;; - :postgres (edd.postgres.*) - PostgreSQL with realm isolation

(ns edd.search
  (:require [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn parse
; TODO parse => build-filter
  [op->filter-builder filter-spec]
  (let [[fst & rst] filter-spec]
    (if (vector? fst)
      (recur op->filter-builder fst)
      (let [builder-fn (get op->filter-builder fst)]
        (apply builder-fn op->filter-builder rst)))))

(defmulti simple-search
  "Searches aggregates by exact field matching within current realm.
   
   Input: ctx with :query (map of field -> value to match)
   Output: Vector of matching aggregates
   
   Uses diff-based filtering - all query fields must match.
   Realm-scoped: Only searches aggregates in current realm."
  (fn [{:keys [query] :as ctx}]
    (:view-store ctx)))

(defmulti advanced-search
  "Complex search with conditions (like, equal, full-text) within current realm.
   
   Input: ctx with search criteria
   Output: Vector of matching aggregates
   
   Supports operators like :like, :equal, :search for flexible querying.
   Realm-scoped: Only searches aggregates in current realm."
  (fn [ctx] (:view-store ctx)))

(defmulti update-aggregate
  "Upserts aggregate (materialized view) in current realm.
   
   Input: ctx with :aggregate (must include :id and :version)
   Output: Updated ctx
   
   Replaces existing aggregate with same ID or creates new.
   Realm-scoped: Aggregates isolated per realm for multi-tenant testing."
  (fn [{:keys [aggregate] :as ctx}]
    (:view-store ctx)))

(defmulti with-init
  "Initializes view store context before processing.
   
   Used for connection setup, index creation, etc.
   Default implementation is a no-op."
  (fn [ctx body-fn]
    (:view-store ctx)))

(defmethod with-init
  :default
  [ctx body-fn]
  (log/info "Default search init")
  (body-fn ctx))

(defmulti get-snapshot
  "Retrieves latest version of aggregate by ID in current realm.
   
   Input: ctx and id (aggregate UUID)
   Output: Aggregate map or nil if not found
   
   Returns highest :version for the given :id.
   Realm-scoped: Only searches current realm."
  (fn [ctx id]
    (:view-store ctx :default)))

(defmethod get-snapshot
  :default
  [ctx id]
  nil)

(defmulti get-by-id-and-version
  "Retrieves specific version of aggregate by ID in current realm.
   
   Input: ctx, id (aggregate UUID), and version (integer)
   Output: Aggregate map or nil if not found
   
   Used for time-travel queries and version history.
   Realm-scoped: Only searches current realm."
  (fn [ctx id version]
    (:view-store ctx :default)))

(def default-size 50)
