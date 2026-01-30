(ns edd.memory.view-store-compliance-test
  "Memory implementation of view store compliance tests.
   Uses in-memory mock DAL for fast unit testing (no external dependencies)."
  (:require [clojure.test :refer [deftest use-fixtures]]
            [edd.compliance.view-store :as compliance]
            [edd.memory.view-store :as view-store]
            [edd.memory.event-store :as event-store]
            [edd.test.fixture.dal :as mock]))

;;; ============================================================================
;;; Test Setup
;;; ============================================================================

(defn make-memory-ctx
  "Creates a memory-based context for testing."
  [& {:keys [realm service-name]
      :or {realm compliance/test-realm
           service-name :test-service}}]
  (-> {:service-name service-name
       :hosted-zone-name "example.com"
       :environment-name-lower "local"
       :meta {:realm realm}}
      (view-store/register)
      (event-store/register)))

(defn with-memory-dal
  "Wraps test with in-memory mock DAL."
  [test-fn]
  (mock/with-mock-dal
    (test-fn)))

;;; ============================================================================
;;; Bind Dynamic Vars and Run All Compliance Tests
;;; ============================================================================

;; Set up fixtures to bind the dynamic vars for all tests in this namespace
(use-fixtures :each
  (fn [test-fn]
    (binding [compliance/*ctx-factory* make-memory-ctx
              compliance/*dal-wrapper* with-memory-dal]
      (test-fn))))

;;; ============================================================================
;;; Re-export All Compliance Tests
;;; ============================================================================

;; Category A: Basic Snapshot Operations
(deftest a1-store-and-get-single-aggregate
  (compliance/a1-store-and-get-single-aggregate))

(deftest a2-update-existing-aggregate
  (compliance/a2-update-existing-aggregate))

(deftest a3-get-non-existent-aggregate-returns-nil
  (compliance/a3-get-non-existent-aggregate-returns-nil))

(deftest a4-store-multiple-aggregates-different-ids
  (compliance/a4-store-multiple-aggregates-different-ids))

(deftest a5-retrieve-latest-version
  (compliance/a5-retrieve-latest-version))

;; Category B: Update Aggregate Operations
(deftest b1-create-new-aggregate
  (compliance/b1-create-new-aggregate))

(deftest b2-replace-existing-aggregate-same-id
  (compliance/b2-replace-existing-aggregate-same-id))

(deftest b3-update-with-incremented-version
  (compliance/b3-update-with-incremented-version))

(deftest b5-update-preserves-all-fields
  (compliance/b5-update-preserves-all-fields))

;; Category C: Version Management
(deftest c2-multiple-versions-same-aggregate
  (compliance/c2-multiple-versions-same-aggregate))

(deftest c3-version-field-properly-maintained
  (compliance/c3-version-field-properly-maintained))

(deftest c4-aggregate-without-version-must-throw
  (compliance/c4-aggregate-without-version-must-throw))

;; Category D: Data Integrity
(deftest d1-aggregate-id-preservation
  (compliance/d1-aggregate-id-preservation))

(deftest d2-complex-nested-data-structures
  (compliance/d2-complex-nested-data-structures))

(deftest d3-special-characters-in-data
  (compliance/d3-special-characters-in-data))

(deftest d4-large-aggregate-payload
  (compliance/d4-large-aggregate-payload))

(deftest d5-minimal-aggregate
  (compliance/d5-minimal-aggregate))

;; Category E: Realm Isolation
(deftest e1-aggregates-in-different-realms-isolated
  (compliance/e1-aggregates-in-different-realms-isolated))

(deftest e2-same-aggregate-id-different-realms
  (compliance/e2-same-aggregate-id-different-realms))

(deftest e3-get-snapshot-scoped-to-realm
  (compliance/e3-get-snapshot-scoped-to-realm))

(deftest e4-update-aggregate-scoped-to-realm
  (compliance/e4-update-aggregate-scoped-to-realm))

;; Category F: Error Handling (STRICT - All MUST throw)
(deftest f1-aggregate-without-id-field-must-throw
  (compliance/f1-aggregate-without-id-field-must-throw))

(deftest f2-nil-aggregate-must-throw
  (compliance/f2-nil-aggregate-must-throw))

(deftest f3-missing-context-must-throw
  (compliance/f3-missing-context-must-throw))

(deftest f4-invalid-id-type-must-throw
  (compliance/f4-invalid-id-type-must-throw))

(deftest f5-invalid-version-type-must-throw
  (compliance/f5-invalid-version-type-must-throw))

(deftest f6-get-snapshot-invalid-id-must-throw
  (compliance/f6-get-snapshot-invalid-id-must-throw))

(deftest f7-get-snapshot-missing-context-must-throw
  (compliance/f7-get-snapshot-missing-context-must-throw))

(deftest f8-update-aggregate-missing-realm-must-throw
  (compliance/f8-update-aggregate-missing-realm-must-throw))

(deftest f9-malformed-aggregate-data-must-throw
  (compliance/f9-malformed-aggregate-data-must-throw))

(deftest f10-concurrent-version-conflict-detection
  (compliance/f10-concurrent-version-conflict-detection))

;; Category G: Concurrent Operations
(deftest g1-multiple-updates-same-aggregate
  (compliance/g1-multiple-updates-same-aggregate))

(deftest g2-multiple-updates-different-aggregates
  (compliance/g2-multiple-updates-different-aggregates))

;; Category H: Versioned Snapshots (CRITICAL REQUIREMENT)
(deftest h1-get-snapshot-by-id-and-version
  (compliance/h1-get-snapshot-by-id-and-version))

(deftest h2-get-non-existent-version-returns-nil
  (compliance/h2-get-non-existent-version-returns-nil))

(deftest h2a-get-non-existent-aggregate-id-with-version-returns-nil
  (compliance/h2a-get-non-existent-aggregate-id-with-version-returns-nil))

(deftest h3-get-version-zero-must-throw
  (compliance/h3-get-version-zero-must-throw))

(deftest h4-versioned-snapshot-realm-isolation
  (compliance/h4-versioned-snapshot-realm-isolation))

(deftest h5-versioned-snapshot-data-integrity
  (compliance/h5-versioned-snapshot-data-integrity))

(deftest h6-get-snapshot-invalid-version-type-must-throw
  (compliance/h6-get-snapshot-invalid-version-type-must-throw))

(deftest h7-versioned-snapshot-performance-large-history
  (compliance/h7-versioned-snapshot-performance-large-history))

;; Category I: Event Store get-events
(deftest i1-get-events-with-nil-version-returns-all
  (compliance/i1-get-events-with-nil-version-returns-all))

(deftest i2-get-events-with-version-filters-older
  (compliance/i2-get-events-with-version-filters-older))

(deftest i3-get-events-without-version-returns-all
  (compliance/i3-get-events-without-version-returns-all))
