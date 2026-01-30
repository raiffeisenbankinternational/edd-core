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
  (fn [{:keys [_query] :as ctx}]
    (:view-store ctx)))

(defmulti advanced-search
  "Complex search with conditions (like, equal, full-text) within current realm.
   
   Input: ctx with search criteria
   Output: Vector of matching aggregates
   
   Supports operators like :like, :equal, :search for flexible querying.
   Realm-scoped: Only searches aggregates in current realm."
  (fn [ctx] (:view-store ctx)))

(defmulti update-aggregate
  "Stores aggregate (materialized view) in current realm.
   
   **Purpose**: Store or update a materialized view of an aggregate after events are applied.
   This is the write operation for the view store in CQRS architecture.
   
   **Input**: 
   - ctx: Context map with :view-store and [:meta :realm]
   - aggregate: Map with :id (UUID) and :version (int), plus other fields
   
   **Output**: 
   - Updated ctx (typically unchanged)
   
   **Behavior**:
   - Replaces existing aggregate with same :id
   - Creates new aggregate if :id doesn't exist
   - MUST store highest :version for get-snapshot to work correctly
   - Implementation may store version history for get-by-id-and-version
   
   **Realm-scoped**: 
   - Aggregates isolated per realm (from ctx [:meta :realm])
   - Same :id in different realms = different aggregates
   
   **Compliance Requirements**:
   - MUST be idempotent (same aggregate, same result)
   - MUST preserve all fields in aggregate
   - MUST use :id as primary key
   - MUST track :version for ordering
   - MUST support realm isolation
   
   **Example**:
   ```clojure
   (update-aggregate ctx {:id #uuid \"...\", :version 3, :name \"Test\"})
   ;; => ctx (aggregate now stored/updated)
   ```"
  (fn [ctx _aggregate]
    (:view-store ctx)))

(defmethod update-aggregate
  :default
  [ctx aggregate]
  ;; Default implementation - validates context/aggregate before failing
  (when-not ctx
    (throw (ex-info "Context cannot be nil for update-aggregate" {})))
  (when-not (get-in ctx [:meta :realm])
    (throw (ex-info "Context must have :realm in [:meta :realm] for update-aggregate"
                    {:ctx ctx})))
  (when-not aggregate
    (throw (ex-info "Aggregate cannot be nil for update-aggregate"
                    {:ctx ctx})))
  (throw (ex-info "No view-store implementation registered"
                  {:view-store (:view-store ctx)
                   :available-methods (keys (methods update-aggregate))})))

(defmulti with-init
  "Initializes view store context before processing.
   
   Used for connection setup, index creation, etc.
   Default implementation is a no-op."
  (fn [ctx _body-fn]
    (:view-store ctx)))

(defmethod with-init
  :default
  [ctx body-fn]
  (log/info "Default search init")
  (body-fn ctx))

(defmulti get-snapshot
  "Retrieves aggregate by ID (optionally at specific version) in current realm.
   
   **Purpose**: Read operation for materialized views - returns the current state
   of an aggregate by replaying all its events (or reading pre-computed view).
   
   **Input**: 
   - ctx: Context map with :view-store and [:meta :realm]
   - id-or-query: Either UUID or map {:id <uuid>, :version <int> (optional)}
   
   **Output**: 
   - Aggregate map with :id, :version, and all stored fields
   - nil if aggregate doesn't exist OR version history not supported
   
   **Behavior**:
   - Without version: Returns aggregate with HIGHEST :version for given :id
   - With version: Returns aggregate at specific version (if supported), nil otherwise
   - Returns nil (not exception) for non-existent :id
   - MUST log warning and return nil if version requested but not supported
   
   **Version History Support**:
    - :postgres - Full support via aggregates-history table
    - :memory - Full support (testing only)
    - :elastic - Full support via history index
    - :s3 - Not supported (logs warning, returns nil)
   
   **Realm-scoped**: 
   - Only searches aggregates in current realm (from ctx [:meta :realm])
   - Same :id in different realms are independent aggregates
   
   **Compliance Requirements**:
   - MUST return nil for non-existent aggregate
   - MUST return highest :version when version not specified
   - MUST preserve all fields from update-aggregate
   - MUST support realm isolation
   - SHOULD be fast (this is the read path in CQRS)
   - MUST log warning and return nil if version requested but not supported
   
   **Example**:
   ```clojure
   ;; Get latest version
   (get-snapshot ctx #uuid \"123e4567-e89b-12d3-a456-426614174000\")
   ;; => {:id #uuid \"...\", :version 5, :name \"Test\", ...}
   
   ;; Get specific version (if supported)
   (get-snapshot ctx {:id #uuid \"123e4567-...\" :version 3})
   ;; => {:id #uuid \"...\", :version 3, :name \"Old\", ...}  ; if supported
   ;; => nil  ; if version history not supported
   
   (get-snapshot ctx #uuid \"non-existent\")
   ;; => nil
   ```"
  (fn [ctx _id-or-query]
    (:view-store ctx :default)))

(defmethod get-snapshot
  :default
  [ctx id-or-query]
  (let [id (if (map? id-or-query) (:id id-or-query) id-or-query)]
    (when-not ctx
      (throw (ex-info "Context cannot be nil for get-snapshot" {:id id})))
    (when-not (get-in ctx [:meta :realm])
      (throw (ex-info "Context must contain [:meta :realm]" {:context ctx :id id})))
    nil))

(defmulti ^{:deprecated "Use (get-snapshot ctx {:id id :version version}) instead"
            :superseded-by "get-snapshot"}
  get-by-id-and-version
  "Retrieves specific version of aggregate by ID in current realm.
   
   Input: ctx, id (aggregate UUID), and version (integer)
   Output: Aggregate map or nil if not found
   
   Used for time-travel queries and version history.
   Realm-scoped: Only searches current realm."
  (fn [ctx _id _version]
    (:view-store ctx :default)))

(def default-size 50)
