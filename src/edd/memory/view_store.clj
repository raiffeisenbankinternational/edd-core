(ns edd.memory.view-store
  "In-memory view store implementation for EDD-Core testing and development.
   
   PREREQUISITES:
   - No external dependencies (pure in-memory storage)
   - Realm must be set in context [:meta :realm] (defaults to :test)
   
   STORAGE ARCHITECTURE:
   - Storage: Atom-based map {:realms {<realm> {:aggregate-store [...] :aggregate-history [...]}}}
   - Realm isolation: All operations scoped to [:meta :realm]
   - Versioning: Latest version in :aggregate-store, all versions in :aggregate-history
   - Matches PostgreSQL: aggregates table (latest) + aggregates_history table (all versions)
   - Thread-safety: Atom-based updates (safe for concurrent tests)
   
   IMPLEMENTATION NOTES:
   - Implements edd.search multimethods: update-aggregate, get-snapshot, get-by-id-and-version
   - STRICT validation: Throws on nil/invalid aggregates, non-UUID IDs, non-integer versions
   - get-snapshot with version retrieves from :aggregate-history (returns nil if not found)
   - get-snapshot without version retrieves from :aggregate-store (latest)
   - Used by edd.test.fixture.dal for unit tests
   
   USAGE:
   (-> ctx (memory-view-store/register))"
  (:require [clojure.data :refer [diff]]
            [clojure.tools.logging :as log]
            [lambda.test.fixture.state :refer [*dal-state*]]
            [edd.memory.search :refer [advanced-search-impl]]
            [edd.memory.event-store :as event-store]
            [edd.search :refer [with-init
                                simple-search
                                advanced-search
                                update-aggregate
                                get-by-id-and-version
                                get-snapshot]]
            [edd.search.validation :as validation]
            [lambda.util :as util]))

(defn fix-keys
  [val]
  (-> val
      (util/to-json)
      (util/to-edn)))

(defn filter-aggregate
  [query aggregate]
  (let [res (diff aggregate query)]
    (and (= (second res) nil)
         (= (nth res 2) query))))

(defmethod simple-search
  :memory
  [{:keys [query] :as ctx}]
  {:pre [query]}
  (into []
        (filter
         #(filter-aggregate
           (dissoc query :query-id)
           %)
         (event-store/get-realm-store ctx :aggregate-store))))

(defn update-aggregate-impl
  [ctx aggregate]
  ;; STRICT VALIDATION - fail fast on any invalid input
  (validation/validate-aggregate! ctx aggregate)

  (util/d-time
   (format "MemoryViewStore update-aggregate, id: %s, version: %s" (:id aggregate) (:version aggregate))
   (let [aggregate (fix-keys aggregate)
         agg-id (:id aggregate)
         agg-version (:version aggregate)]
     ;; Store in history before updating current aggregate
     (event-store/update-realm-store!
      ctx
      :aggregate-history
      (fn [history]
        ;; Add to history (keep all versions like aggregates_history table)
        ;; Remove existing entry with same ID+version (idempotency)
        (->> history
             (remove (fn [el] (and (= (:id el) agg-id)
                                   (= (:version el) agg-version))))
             (cons aggregate)
             (sort-by (fn [{:keys [id version]}] [(str id) version])))))

     ;; Update current aggregate (only latest version, like aggregates table)
     (event-store/update-realm-store!
      ctx
      :aggregate-store
      (fn [v]
        ;; Keep only the LATEST version per aggregate ID (like production stores)
        ;; Remove any existing aggregate with same ID, then add new version
        ;; This matches PostgreSQL behavior where only current state is stored
        (->> v
             (remove (fn [el] (= (:id el) agg-id)))
             (cons aggregate)
             (sort-by (fn [{:keys [id version]}] [(str id) version])))))))
  ctx)

(defmethod update-aggregate
  :memory
  [ctx aggregate]
  (update-aggregate-impl ctx aggregate))

(defmethod advanced-search
  :memory
  [ctx]
  (advanced-search-impl ctx))

(defmethod with-init
  :memory
  [ctx body-fn]
  (log/debug "Initializing memory view store")
  (if-not (:global @*dal-state*)
    (body-fn ctx)
    (binding [*dal-state* (atom {})]
      (body-fn ctx))))

(defn- aggregates-matching-id
  "Get aggregates from aggregate-store (latest only) matching ID"
  [ctx id]
  (->> (event-store/get-realm-store ctx :aggregate-store)
       (filter #(= (:id %) id))))

(defn- history-matching-id-and-version
  "Get historical aggregate from aggregate-history matching ID and version"
  [ctx id version]
  (->> (event-store/get-realm-store ctx :aggregate-history)
       (filter #(and (= (:id %) id)
                     (= (:version %) version)))
       first))

(defmethod get-snapshot
  :memory
  [ctx id-or-query]
  (let [{:keys [id version]} (validation/validate-snapshot-query! ctx id-or-query)]
    (if (some? version)
      ;; Specific version requested - retrieve from history
      (util/d-time
       (format "MemoryViewStore get-snapshot, id: %s, version: %s (from history)" id version)
       (history-matching-id-and-version ctx id version))
      ;; Latest version - retrieve from aggregate-store
      (util/d-time
       (format "MemoryViewStore get-snapshot, id: %s (latest)" id)
       (->> (aggregates-matching-id ctx id)
            first)))))

(defmethod get-by-id-and-version
  :memory
  [ctx id version]
  (validation/validate-id-and-version! ctx id version)
  ;; Delegate to get-snapshot
  (get-snapshot ctx {:id id :version version}))

(defn register
  [ctx]
  (assoc ctx :view-store :memory))
