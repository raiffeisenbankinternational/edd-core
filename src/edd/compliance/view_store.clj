(ns edd.compliance.view-store
  "Compliance test suite for view store implementations.
   Tests that all implementations (Memory, Postgres) behave identically for
   get-snapshot and update-aggregate operations.
   
   SCOPE: Tests only get-snapshot and update-aggregate (search functions deferred)
   
   This namespace provides test definitions that are NOT run directly.
   Implementations should require this namespace and invoke the test functions
   with proper context bindings.
   
   Test implementations dispatch on dynamic vars:
   - *ctx-factory* - Function to create context: (fn [& args] ctx)
   - *dal-wrapper* - Function to wrap tests with DAL: (fn [test-fn] ...)
   
   Category I (event store) tests use DAL multimethods (dal/store-results,
   dal/get-events) and auto-detect event store availability via :edd-event-store
   key in context."
  (:require [clojure.test :refer [deftest is testing]]
            [edd.dal :as dal]
            [edd.search :as search]
            [lambda.uuid :as uuid]))

;;; ============================================================================
;;; Test Infrastructure
;;; ============================================================================

(def test-realm :test)  ; Default realm for single-realm tests

(def ^:dynamic *ctx-factory*
  "Dynamic var for context factory function.
   Implementations should bind this to their context creation function.
   Signature: (fn [& {:keys [realm service-name]}] ctx)"
  nil)

(def ^:dynamic *dal-wrapper*
  "Dynamic var for DAL wrapper function.
   Implementations should bind this to their DAL setup/teardown function.
   Signature: (fn [test-fn] ...)"
  nil)

(defn make-ctx
  "Creates a base context for testing with optional overrides.
   Uses *ctx-factory* to create implementation-specific context.
   Adds request-id, interaction-id, invocation-id and breadcrumbs
   needed by dal/store-results for Category I event store tests."
  [& {:keys [realm service-name]
      :or {realm test-realm
           service-name :test-service}}]
  (if *ctx-factory*
    (let [ctx (*ctx-factory* :realm realm :service-name service-name)]
      (merge {:request-id (uuid/gen)
              :interaction-id (uuid/gen)
              :invocation-id (uuid/gen)
              :breadcrumbs [0]}
             ctx))
    (throw (ex-info "No *ctx-factory* bound. Implementation tests must bind this var."
                    {:realm realm :service-name service-name}))))

(defn make-aggregate
  "Creates a test aggregate with required fields."
  [& {:keys [id version attrs]
      :or {id (uuid/gen)
           version 1
           attrs {}}}]
  (merge {:id id
          :version version}
         attrs))

(defn validate-aggregate
  "Validates that actual aggregate matches expected aggregate.
   Compares :id and :version, plus any additional fields in expected."
  [actual expected & {:keys [msg]
                      :or {msg "Aggregate should match expected"}}]
  (is (not (nil? actual)) (str msg " - should not be nil"))
  (when actual
    (is (= (:id expected) (:id actual)) (str msg " - :id should match"))
    (is (= (:version expected) (:version actual)) (str msg " - :version should match"))
    ;; Validate all additional fields from expected
    (doseq [[k v] (dissoc expected :id :version)]
      (is (= v (get actual k)) (str msg " - field " k " should match")))))

(defn store-aggregate
  "Stores an aggregate using update-aggregate."
  [ctx aggregate]
  (search/update-aggregate ctx aggregate))

;; Get aggregate by ID only (latest version)
(defn get-aggregate
  "Retrieves latest version of aggregate using get-snapshot."
  [ctx id]
  (search/get-snapshot ctx id))

;; Get aggregate by ID and specific version using unified interface
(defn get-aggregate-version
  "Retrieves specific version of aggregate using get-snapshot with version parameter.
   Returns nil if version doesn't exist."
  [ctx id version]
  (search/get-snapshot ctx {:id id :version version}))

;;; ============================================================================
;;; Category A: Basic Snapshot Operations
;;; ============================================================================

(deftest a1-store-and-get-single-aggregate
  (testing
   "A1: Store a single aggregate and retrieve with get-snapshot"
    (*dal-wrapper*
     (fn []
       (let [ctx (make-ctx)
             agg-id (uuid/gen)
             aggregate (make-aggregate :id agg-id :version 1 :attrs {:name "Test User"})]
         (store-aggregate ctx aggregate)
         (let [retrieved (get-aggregate ctx agg-id)]
           (is (not (nil? retrieved)) "Retrieved aggregate should not be nil")
           (validate-aggregate retrieved aggregate)
           (is (= "Test User" (:name retrieved)))))))))

(deftest a2-update-existing-aggregate
  (testing
   "A2: Update existing aggregate"
    (*dal-wrapper*
     (fn []
       (let [ctx (make-ctx)
             agg-id (uuid/gen)
             aggregate-v1 (make-aggregate :id agg-id :version 1 :attrs {:name "First"})
             aggregate-v2 (make-aggregate :id agg-id :version 2 :attrs {:name "Second"})]
       ;; Store version 1
         (store-aggregate ctx aggregate-v1)
         (let [retrieved-v1 (get-aggregate ctx agg-id)]
           (is (= "First" (:name retrieved-v1))))
       ;; Update to version 2
         (store-aggregate ctx aggregate-v2)
         (let [retrieved-v2 (get-aggregate ctx agg-id)]
           (validate-aggregate retrieved-v2 aggregate-v2)
           (is (= "Second" (:name retrieved-v2)))))))))

(deftest a3-get-non-existent-aggregate-returns-nil
  "A3: Get non-existent aggregate returns nil"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           non-existent-id (uuid/gen)
           retrieved (get-aggregate ctx non-existent-id)]
       (is (nil? retrieved) "Non-existent aggregate should return nil")))))

(deftest a4-store-multiple-aggregates-different-ids
  "A4: Store multiple aggregates with different IDs"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id-1 (uuid/gen)
           agg-id-2 (uuid/gen)
           agg-id-3 (uuid/gen)
           aggregate-1 (make-aggregate :id agg-id-1 :version 1 :attrs {:name "First"})
           aggregate-2 (make-aggregate :id agg-id-2 :version 1 :attrs {:name "Second"})
           aggregate-3 (make-aggregate :id agg-id-3 :version 1 :attrs {:name "Third"})]
       ;; Store all three
       (store-aggregate ctx aggregate-1)
       (store-aggregate ctx aggregate-2)
       (store-aggregate ctx aggregate-3)
       ;; Retrieve and validate each
       (let [retrieved-1 (get-aggregate ctx agg-id-1)
             retrieved-2 (get-aggregate ctx agg-id-2)
             retrieved-3 (get-aggregate ctx agg-id-3)]
         (validate-aggregate retrieved-1 aggregate-1)
         (validate-aggregate retrieved-2 aggregate-2)
         (validate-aggregate retrieved-3 aggregate-3))))))

(deftest a5-retrieve-latest-version
  "A5: Retrieve latest version of aggregate"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate (make-aggregate :id agg-id :version 5 :attrs {:name "Latest"})]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= 5 (:version retrieved)) "Should return version 5"))))))

;;; ============================================================================
;;; Category B: Update Aggregate Operations
;;; ============================================================================

(deftest b1-create-new-aggregate
  "B1: Create new aggregate with update-aggregate"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate (make-aggregate :id agg-id :version 1 :attrs {:status :active})]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (validate-aggregate retrieved aggregate)
         (is (= :active (:status retrieved))))))))

(deftest b2-replace-existing-aggregate-same-id
  "B2: Replace existing aggregate with same ID"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate-old (make-aggregate :id agg-id :version 1 :attrs {:value 100})
           aggregate-new (make-aggregate :id agg-id :version 2 :attrs {:value 200})]
       (store-aggregate ctx aggregate-old)
       (store-aggregate ctx aggregate-new)
       (let [retrieved (get-aggregate ctx agg-id)]
         (validate-aggregate retrieved aggregate-new)
         (is (= 200 (:value retrieved))))))))

(deftest b3-update-with-incremented-version
  "B3: Update aggregate with incremented version"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Store versions 1, 2, 3
       (store-aggregate ctx (make-aggregate :id agg-id :version 1))
       (store-aggregate ctx (make-aggregate :id agg-id :version 2))
       (store-aggregate ctx (make-aggregate :id agg-id :version 3))
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= 3 (:version retrieved)) "Should return highest version"))))))

(deftest b5-update-preserves-all-fields
  "B5: Update aggregate preserves all fields"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate (make-aggregate :id agg-id
                                     :version 1
                                     :attrs {:name "Test"
                                             :email "test@example.com"
                                             :status :active
                                             :count 42
                                             :tags ["a" "b" "c"]
                                             :metadata {:key "value"}})]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (validate-aggregate retrieved aggregate)
         (is (= "Test" (:name retrieved)))
         (is (= "test@example.com" (:email retrieved)))
         (is (= :active (:status retrieved)))
         (is (= 42 (:count retrieved)))
         (is (= ["a" "b" "c"] (:tags retrieved)))
         (is (= {:key "value"} (:metadata retrieved))))))))

;;; ============================================================================
;;; Category C: Version Management
;;; ============================================================================

(deftest c2-multiple-versions-same-aggregate
  "C2: Multiple versions of same aggregate (last write wins)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       (doseq [v (range 1 6)]
         (store-aggregate ctx (make-aggregate :id agg-id :version v :attrs {:step v})))
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= 5 (:version retrieved)))
         (is (= 5 (:step retrieved))))))))

(deftest c3-version-field-properly-maintained
  "C3: Version field is properly maintained"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate (make-aggregate :id agg-id :version 7)]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (contains? retrieved :version) "Retrieved aggregate should have :version")
         (is (= 7 (:version retrieved))))))))

(deftest c4-aggregate-without-version-must-throw
  "C4: Aggregate without version field MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate {:id agg-id :name "No Version"}]  ;; Missing :version
       (is (thrown? Exception
                    (store-aggregate ctx aggregate))
           "MUST throw exception for aggregate without :version field")))))

;;; ============================================================================
;;; Category D: Data Integrity
;;; ============================================================================

(deftest d1-aggregate-id-preservation
  "D1: Aggregate ID preservation"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate (make-aggregate :id agg-id :version 1)]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= agg-id (:id retrieved)) "ID should be preserved exactly"))))))

(deftest d2-complex-nested-data-structures
  "D2: Complex nested data structures"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           complex-data {:users [{:id 1 :name "Alice" :roles [:admin :user]}
                                 {:id 2 :name "Bob" :roles [:user]}]
                         :settings {:theme "dark"
                                    :notifications {:email true :sms false}}
                         :metadata {:created-at "2024-01-01"
                                    :tags ["important" "urgent"]}}
           aggregate (make-aggregate :id agg-id :version 1 :attrs complex-data)]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= complex-data (dissoc retrieved :id :version))
             "Complex nested structure should be preserved"))))))

(deftest d3-special-characters-in-data
  "D3: Special characters in aggregate data"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           special-text "Test with special chars: @#$%^&*()_+-=[]{}|;':\",./<>?"
           aggregate (make-aggregate :id agg-id :version 1 :attrs {:text special-text})]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= special-text (:text retrieved))
             "Special characters should be preserved"))))))

(deftest d4-large-aggregate-payload
  "D4: Large aggregate payload (e.g., 1000 items)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           large-items (vec (for [i (range 1000)]
                              {:id i :value (str "Item-" i)}))
           aggregate (make-aggregate :id agg-id :version 1 :attrs {:items large-items})]
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= 1000 (count (:items retrieved))) "Should store and retrieve 1000 items")
         (is (= large-items (:items retrieved)) "Large payload should be preserved"))))))

(deftest d5-minimal-aggregate
  "D5: Empty aggregate (minimal fields: just :id and :version)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           aggregate {:id agg-id :version 1}]  ; Minimal aggregate
       (store-aggregate ctx aggregate)
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (not (nil? retrieved)))
         (is (= agg-id (:id retrieved)))
         (is (= 1 (:version retrieved))))))))

;;; ============================================================================
;;; Category E: Realm Isolation
;;; ============================================================================

(deftest e1-aggregates-in-different-realms-isolated
  "E1: Aggregates in different realms are isolated"
  (*dal-wrapper*
   (fn []
     (let [ctx-test (make-ctx :realm :test)
           ctx-prod (make-ctx :realm :prod)
           agg-id (uuid/gen)
           agg-test (make-aggregate :id agg-id :version 1 :attrs {:realm-name "test"})
           agg-prod (make-aggregate :id agg-id :version 1 :attrs {:realm-name "prod"})]
       ;; Store in test realm
       (store-aggregate ctx-test agg-test)
       ;; Store in prod realm
       (store-aggregate ctx-prod agg-prod)
       ;; Retrieve from each realm
       (let [retrieved-test (get-aggregate ctx-test agg-id)
             retrieved-prod (get-aggregate ctx-prod agg-id)]
         ;; Each realm should see only its own data
         (is (= "test" (:realm-name retrieved-test)))
         (is (= "prod" (:realm-name retrieved-prod))))))))

(deftest e2-same-aggregate-id-different-realms
  "E2: Same aggregate ID in different realms"
  (*dal-wrapper*
   (fn []
     (let [shared-id (uuid/gen)
           ctx-realm-a (make-ctx :realm :realm-a)
           ctx-realm-b (make-ctx :realm :realm-b)
           agg-a (make-aggregate :id shared-id :version 1 :attrs {:value "A"})
           agg-b (make-aggregate :id shared-id :version 2 :attrs {:value "B"})]
       (store-aggregate ctx-realm-a agg-a)
       (store-aggregate ctx-realm-b agg-b)
       (let [retrieved-a (get-aggregate ctx-realm-a shared-id)
             retrieved-b (get-aggregate ctx-realm-b shared-id)]
         (is (= "A" (:value retrieved-a)))
         (is (= 1 (:version retrieved-a)))
         (is (= "B" (:value retrieved-b)))
         (is (= 2 (:version retrieved-b))))))))

(deftest e3-get-snapshot-scoped-to-realm
  "E3: get-snapshot scoped to realm"
  (*dal-wrapper*
   (fn []
     (let [agg-id (uuid/gen)
           ctx-realm-x (make-ctx :realm :realm-x)
           ctx-realm-y (make-ctx :realm :realm-y)
           aggregate (make-aggregate :id agg-id :version 1 :attrs {:data "X"})]
       ;; Store only in realm-x
       (store-aggregate ctx-realm-x aggregate)
       ;; Get from realm-x should succeed
       (let [retrieved-x (get-aggregate ctx-realm-x agg-id)]
         (is (not (nil? retrieved-x))))
       ;; Get from realm-y should return nil
       (let [retrieved-y (get-aggregate ctx-realm-y agg-id)]
         (is (nil? retrieved-y) "Should not see aggregate from different realm"))))))

(deftest e4-update-aggregate-scoped-to-realm
  "E4: update-aggregate scoped to realm"
  (*dal-wrapper*
   (fn []
     (let [agg-id (uuid/gen)
           ctx-realm-1 (make-ctx :realm :realm-1)
           ctx-realm-2 (make-ctx :realm :realm-2)
           agg-1 (make-aggregate :id agg-id :version 1 :attrs {:count 10})
           agg-2 (make-aggregate :id agg-id :version 1 :attrs {:count 20})]
       ;; Update in both realms
       (store-aggregate ctx-realm-1 agg-1)
       (store-aggregate ctx-realm-2 agg-2)
       ;; Each realm maintains its own version
       (let [retrieved-1 (get-aggregate ctx-realm-1 agg-id)
             retrieved-2 (get-aggregate ctx-realm-2 agg-id)]
         (is (= 10 (:count retrieved-1)))
         (is (= 20 (:count retrieved-2))))))))

;;; ============================================================================
;;; Category F: Error Handling (STRICT - All errors must throw exceptions)
;;; ============================================================================

(deftest f1-aggregate-without-id-field-must-throw
  "F1: Aggregate without :id field MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           invalid-aggregate {:version 1 :name "No ID"}]
       (is (thrown? Exception
                    (store-aggregate ctx invalid-aggregate))
           "MUST throw exception for aggregate without :id field")))))

(deftest f2-nil-aggregate-must-throw
  "F2: Nil aggregate MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)]
       (is (thrown? Exception
                    (search/update-aggregate ctx nil))
           "MUST throw exception for nil aggregate")))))

(deftest f3-missing-context-must-throw
  "F3: Missing or nil context MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [aggregate (make-aggregate)]
       (is (thrown? Exception
                    (search/update-aggregate nil aggregate))
           "MUST throw exception for nil context")
       (is (thrown? Exception
                    (search/update-aggregate {} aggregate))
           "MUST throw exception for context missing :realm")))))

(deftest f4-invalid-id-type-must-throw
  "F4: Invalid ID type (non-UUID) MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)]
       (is (thrown? Exception
                    (store-aggregate ctx {:id "not-a-uuid" :version 1}))
           "MUST throw exception for string ID")
       (is (thrown? Exception
                    (store-aggregate ctx {:id 12345 :version 1}))
           "MUST throw exception for numeric ID")))))

(deftest f5-invalid-version-type-must-throw
  "F5: Invalid version type MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       (is (thrown? Exception
                    (store-aggregate ctx {:id agg-id :version "not-a-number"}))
           "MUST throw exception for string version")
       (is (thrown? Exception
                    (store-aggregate ctx {:id agg-id :version -1}))
           "MUST throw exception for negative version")
       (is (thrown? Exception
                    (store-aggregate ctx {:id agg-id :version 0}))
           "MUST throw exception for version 0 (version must be > 0)")))))

(deftest f6-get-snapshot-invalid-id-must-throw
  "F6: get-snapshot with invalid ID MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)]
       (is (thrown? Exception
                    (search/get-snapshot ctx "not-a-uuid"))
           "MUST throw exception for non-UUID ID")
       (is (thrown? Exception
                    (search/get-snapshot ctx nil))
           "MUST throw exception for nil ID")))))

(deftest f7-get-snapshot-missing-context-must-throw
  "F7: get-snapshot with missing context MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [agg-id (uuid/gen)]
       (is (thrown? Exception
                    (search/get-snapshot nil agg-id))
           "MUST throw exception for nil context")
       (is (thrown? Exception
                    (search/get-snapshot {} agg-id))
           "MUST throw exception for context missing :realm")))))

(deftest f8-update-aggregate-missing-realm-must-throw
  "F8: update-aggregate without realm in context MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [aggregate (make-aggregate)]
       (is (thrown? Exception
                    (search/update-aggregate {} aggregate))
           "MUST throw exception when context missing :realm")))))

(deftest f9-malformed-aggregate-data-must-throw
  "F9: Malformed aggregate data MUST throw exception or preserve exactly"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Aggregate with :id but wrong type structure
       (is (thrown? Exception
                    (store-aggregate ctx {:id agg-id :version 1 :data (Object.)}))
           "MUST throw exception for non-serializable data")))))

(deftest f10-concurrent-version-conflict-detection
  "F10: Concurrent updates with same version should be detectable"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Store v1
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs {:value "first"}))
       ;; Attempt to store v1 again (simulating concurrent write)
       ;; Implementation should either:
       ;; 1. Allow it (last-write-wins) 
       ;; 2. Throw optimistic lock exception
       ;; But MUST NOT silently corrupt data
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs {:value "concurrent"}))
       (let [retrieved (get-aggregate ctx agg-id)]
         ;; Must be one of the two values, never corrupted/merged
         (is (contains? #{"first" "concurrent"} (:value retrieved))
             "MUST NOT corrupt data on concurrent writes"))))))

;;; ============================================================================
;;; Category G: Concurrent Operations
;;; ============================================================================

(deftest g1-multiple-updates-same-aggregate
  "G1: Multiple updates to same aggregate (last write wins)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Simulate multiple sequential updates
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs {:value "first"}))
       (store-aggregate ctx (make-aggregate :id agg-id :version 2 :attrs {:value "second"}))
       (store-aggregate ctx (make-aggregate :id agg-id :version 3 :attrs {:value "third"}))
       (let [retrieved (get-aggregate ctx agg-id)]
         (is (= "third" (:value retrieved)) "Last write should win"))))))

(deftest g2-multiple-updates-different-aggregates
  "G2: Multiple updates to different aggregates"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           ids (repeatedly 5 uuid/gen)]
       ;; Store multiple aggregates
       (doseq [[idx id] (map-indexed vector ids)]
         (store-aggregate ctx (make-aggregate :id id :version 1 :attrs {:index idx})))
       ;; Verify all are stored correctly
       (doseq [[idx id] (map-indexed vector ids)]
         (let [retrieved (get-aggregate ctx id)]
           (is (= idx (:index retrieved)))))))))

;;; ============================================================================
;;; Category H: Versioned Snapshot Retrieval (CRITICAL REQUIREMENT)
;;; ============================================================================

(deftest h1-get-snapshot-by-id-and-version
  "H1: Retrieve specific version of aggregate (REQUIRED for event sourcing)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Store versions 1, 2, 3 with different data
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs {:value "v1" :step 1}))
       (store-aggregate ctx (make-aggregate :id agg-id :version 2 :attrs {:value "v2" :step 2}))
       (store-aggregate ctx (make-aggregate :id agg-id :version 3 :attrs {:value "v3" :step 3}))

       ;; Get latest (2-arity: ctx + id only)
       (let [latest (get-aggregate ctx agg-id)]
         (is (= 3 (:version latest)) "Latest should be version 3")
         (is (= "v3" (:value latest))))

       ;; Get specific versions (3-arity: ctx + id + version)
       ;; Each MUST return exact version or nil if missing
       (let [v1 (get-aggregate-version ctx agg-id 1)
             v2 (get-aggregate-version ctx agg-id 2)
             v3 (get-aggregate-version ctx agg-id 3)]
         (is (not (nil? v1)) "Version 1 should exist")
         (is (= 1 (:version v1)) "Should retrieve version 1")
         (is (= "v1" (:value v1)))
         (is (= 1 (:step v1)))

         (is (not (nil? v2)) "Version 2 should exist")
         (is (= 2 (:version v2)) "Should retrieve version 2")
         (is (= "v2" (:value v2)))
         (is (= 2 (:step v2)))

         (is (not (nil? v3)) "Version 3 should exist")
         (is (= 3 (:version v3)) "Should retrieve version 3")
         (is (= "v3" (:value v3)))
         (is (= 3 (:step v3))))))))

(deftest h2-get-non-existent-version-returns-nil
  "H2: Request version that doesn't exist returns nil"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Store only versions 1 and 3 (skip 2)
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs {:value "v1"}))
       (store-aggregate ctx (make-aggregate :id agg-id :version 3 :attrs {:value "v3"}))

       ;; Version 2 was never stored - should return nil
       (let [v2 (get-aggregate-version ctx agg-id 2)]
         (is (nil? v2) "Should return nil when requesting non-existent version 2"))

       ;; Version 10 (future version) - should return nil
       (let [v10 (get-aggregate-version ctx agg-id 10)]
         (is (nil? v10) "Should return nil when requesting future version"))

       ;; Latest should still work (no version specified)
       (let [latest (get-aggregate ctx agg-id)]
         (is (= 3 (:version latest)) "Latest should return highest version"))))))

(deftest h2a-get-non-existent-aggregate-id-with-version-returns-nil
  "H2a: Request version for aggregate ID that doesn't exist returns nil"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           non-existent-id (uuid/gen)]
       ;; Never stored this ID

       ;; Requesting specific version of non-existent aggregate returns nil
       (let [v1 (get-aggregate-version ctx non-existent-id 1)]
         (is (nil? v1) "Should return nil when aggregate ID doesn't exist"))

       ;; Latest version of non-existent ID also returns nil (consistent behavior)
       (let [latest (get-aggregate ctx non-existent-id)]
         (is (nil? latest) "Latest of non-existent ID returns nil"))))))

(deftest h3-get-version-zero-must-throw
  "H3: Version 0 in query MUST throw exception (versions must be > 0)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       ;; Store a valid aggregate first
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs {:value "v1"}))
       ;; Version 0 in query MUST throw
       (is (thrown? Exception
                    (search/get-snapshot ctx {:id agg-id :version 0}))
           "MUST throw exception for version 0 in query")))))

(deftest h4-versioned-snapshot-realm-isolation
  "H4: Versioned snapshots MUST respect realm boundaries"
  (*dal-wrapper*
   (fn []
     (let [shared-id (uuid/gen)
           ctx-a (make-ctx :realm :realm-a)
           ctx-b (make-ctx :realm :realm-b)]

       ;; Store v1 and v2 in realm-a
       (store-aggregate ctx-a (make-aggregate :id shared-id :version 1 :attrs {:realm "a" :value "a1"}))
       (store-aggregate ctx-a (make-aggregate :id shared-id :version 2 :attrs {:realm "a" :value "a2"}))

       ;; Store v1 and v3 in realm-b (different version sequence)
       (store-aggregate ctx-b (make-aggregate :id shared-id :version 1 :attrs {:realm "b" :value "b1"}))
       (store-aggregate ctx-b (make-aggregate :id shared-id :version 3 :attrs {:realm "b" :value "b3"}))

       ;; Realm-a should see v1, v2
       (let [v1-a (get-aggregate-version ctx-a shared-id 1)
             v2-a (get-aggregate-version ctx-a shared-id 2)]
         (is (= "a1" (:value v1-a)) "Realm-a should see its v1")
         (is (= "a2" (:value v2-a)) "Realm-a should see its v2"))

       ;; Realm-a should NOT see v3 (doesn't exist in this realm) - should return nil
       (is (nil?
            (get-aggregate-version ctx-a shared-id 3))
           "Realm-a should return nil when requesting v3 (exists only in realm-b)")

       ;; Realm-b should see v1, v3
       (let [v1-b (get-aggregate-version ctx-b shared-id 1)
             v3-b (get-aggregate-version ctx-b shared-id 3)]
         (is (= "b1" (:value v1-b)) "Realm-b should see its v1")
         (is (= "b3" (:value v3-b)) "Realm-b should see its v3"))

        ;; Realm-b should NOT see v2 (doesn't exist in this realm) - should return nil
       (is (nil?
            (get-aggregate-version ctx-b shared-id 2))
           "Realm-b should return nil when requesting v2 (exists only in realm-a)")))))

(deftest h5-versioned-snapshot-data-integrity
  "H5: Historical versions preserve exact data (immutability check)"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           complex-v1 {:users [{:id 1 :name "Alice"}] :metadata {:created "2024-01-01"}}
           complex-v2 {:users [{:id 1 :name "Alice"} {:id 2 :name "Bob"}] :metadata {:created "2024-01-01" :updated "2024-01-02"}}]

       ;; Store two versions with complex data
       (store-aggregate ctx (make-aggregate :id agg-id :version 1 :attrs complex-v1))
       (store-aggregate ctx (make-aggregate :id agg-id :version 2 :attrs complex-v2))

        ;; Retrieve v1 - should be unchanged by v2
       (let [v1 (get-aggregate-version ctx agg-id 1)]
         (is (= complex-v1 (dissoc v1 :id :version))
             "Version 1 should be immutable (not affected by v2 update)"))

        ;; Retrieve v2
       (let [v2 (get-aggregate-version ctx agg-id 2)]
         (is (= complex-v2 (dissoc v2 :id :version))
             "Version 2 should have new data"))))))

(deftest h6-get-snapshot-invalid-version-type-must-throw
  "H6: get-by-id-and-version with invalid version type MUST throw exception"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)]
       (store-aggregate ctx (make-aggregate :id agg-id :version 1))

       ;; String version - MUST throw
       (is (thrown? Exception
                    (get-aggregate-version ctx agg-id "not-a-number"))
           "MUST throw exception for string version")

       ;; Negative version - MUST throw
       (is (thrown? Exception
                    (get-aggregate-version ctx agg-id -1))
           "MUST throw exception for negative version")

       ;; Float version - MUST throw (versions must be integers)
       (is (thrown? Exception
                    (get-aggregate-version ctx agg-id 1.5))
           "MUST throw exception for float version")))))

(deftest h7-versioned-snapshot-performance-large-history
  "H7: Retrieve specific version from aggregate with many versions"
  (*dal-wrapper*
   (fn []
     (let [ctx (make-ctx)
           agg-id (uuid/gen)
           num-versions 100]

       ;; Store 100 versions
       (doseq [v (range 1 (inc num-versions))]
         (store-aggregate ctx (make-aggregate :id agg-id :version v :attrs {:step v :data (str "Version-" v)})))

       ;; Retrieve version 1 (oldest)
       (let [v1 (get-aggregate-version ctx agg-id 1)]
         (is (= 1 (:version v1)))
         (is (= 1 (:step v1))))

       ;; Retrieve version 50 (middle)
       (let [v50 (get-aggregate-version ctx agg-id 50)]
         (is (= 50 (:version v50)))
         (is (= 50 (:step v50))))

       ;; Retrieve version 100 (latest via versioned lookup)
       (let [v100 (get-aggregate-version ctx agg-id 100)]
         (is (= 100 (:version v100)))
         (is (= 100 (:step v100))))

       ;; Latest (via 2-arity) should match v100
       (let [latest (get-aggregate ctx agg-id)]
         (is (= 100 (:version latest)))
         (is (= 100 (:step latest))))))))

;;; ============================================================================
;;; Category I: Event Store get-events (requires :edd-event-store in context)
;;; ============================================================================

(defn- has-event-store?
  "Returns true if the context has an event store registered."
  [ctx]
  (some? (:edd-event-store ctx)))

(defn- store-events
  "Stores events via dal/store-results, matching the pattern from
   event_store_compliance_test.clj."
  [ctx events]
  (let [enriched-events (mapv #(assoc %
                                      :interaction-id (:interaction-id ctx)
                                      :request-id (:request-id ctx))
                              events)
        ctx-with-resp (assoc ctx :resp {:events enriched-events
                                        :effects []
                                        :identities []
                                        :summary {}})]
    (dal/store-results ctx-with-resp)))

(deftest i1-get-events-with-nil-version-returns-all
  (testing "I1: get-events with nil version returns all events (snapshot not found case)"
    (*dal-wrapper*
     (fn []
       (let [ctx (make-ctx)]
         (when (has-event-store? ctx)
           (let [agg-id (uuid/gen)]
             (store-events ctx [{:id agg-id :event-seq 1 :event-id :first}
                                {:id agg-id :event-seq 2 :event-id :second}
                                {:id agg-id :event-seq 3 :event-id :third}])
             ;; nil version is what happens when no snapshot exists
             (let [events (dal/get-events (assoc ctx :id agg-id :version nil))]
               (is (= 3 (count events)) "nil version must return all events")
               (is (= [1 2 3] (mapv :event-seq events)))))))))))

(deftest i2-get-events-with-version-filters-older
  (testing "I2: get-events with version only returns events after that version"
    (*dal-wrapper*
     (fn []
       (let [ctx (make-ctx)]
         (when (has-event-store? ctx)
           (let [agg-id (uuid/gen)]
             (store-events ctx [{:id agg-id :event-seq 1 :event-id :first}
                                {:id agg-id :event-seq 2 :event-id :second}
                                {:id agg-id :event-seq 3 :event-id :third}])
             (let [events (dal/get-events (assoc ctx :id agg-id :version 1))]
               (is (= 2 (count events)))
               (is (= [2 3] (mapv :event-seq events)))))))))))

(deftest i3-get-events-without-version-returns-all
  (testing "I3: get-events without version returns all events"
    (*dal-wrapper*
     (fn []
       (let [ctx (make-ctx)]
         (when (has-event-store? ctx)
           (let [agg-id (uuid/gen)]
             (store-events ctx [{:id agg-id :event-seq 1 :event-id :first}
                                {:id agg-id :event-seq 2 :event-id :second}])
             (let [events (dal/get-events (assoc ctx :id agg-id :version nil))]
               (is (= 2 (count events)) "No version must return all events")
               (is (= [1 2] (mapv :event-seq events)))))))))))

