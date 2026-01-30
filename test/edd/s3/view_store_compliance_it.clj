(ns edd.s3.view-store-compliance-it
  "S3 implementation of view store compliance tests.
   Uses real AWS S3 (integration tests).
   
   IMPORTANT: This is an INTEGRATION TEST (_it.clj suffix)
   - Requires AWS credentials to be configured
   - Requires environment variables: AccountId, EnvironmentNameLower
   - Creates S3 bucket: <AccountId>-<EnvironmentNameLower>-aggregates
   - Run with: make it
   
   The S3 view store stores aggregates as JSON files in S3:
   - Latest: s3://<bucket>/aggregates/<realm>/latest/<service>/<partition>/<id>.json
   - History: s3://<bucket>/aggregates/<realm>/history/<service>/<partition>/<id>/<version>.json
   - Versioning: Full history preserved (all versions stored)
   - Realm isolation via path prefix"
  (:require [clojure.test :refer [deftest use-fixtures testing is]]
            [edd.compliance.view-store :as compliance]
            [edd.s3.view-store :as view-store]
            [lambda.util :as util]
            [aws.ctx :as aws-ctx]
            [sdk.aws.s3 :as s3]
            [lambda.uuid :as uuid]))

;;; ============================================================================
;;; Test Setup
;;; ============================================================================

(def ^:dynamic *test-ctx* nil)

(defn make-s3-ctx
  "Creates an S3-based context for testing."
  [& {:keys [realm service-name]
      :or {realm compliance/test-realm
           service-name :test-service}}]
  (-> *test-ctx*
      (assoc :service-name service-name
             :meta {:realm realm})
      (view-store/register)))

(defn with-s3-dal
  "Wraps test with S3 context.
   S3 connection is established by fixtures."
  [test-fn]
  ;; Tests run within the *test-ctx* binding established by fixtures
  (test-fn))

;;; ============================================================================
;;; Fixtures
;;; ============================================================================

(defn setup-s3-ctx
  "Initialize AWS context for S3 operations.
   Uses aws.ctx/init to set up AWS configuration from environment variables."
  [test-fn]
  (let [env-name (util/get-env "EnvironmentNameLower")

        ;; Validate EnvironmentNameLower is set (needed for S3 bucket naming)
        _ (when-not env-name
            (throw (ex-info "S3 integration test requires EnvironmentNameLower environment variable"
                            {:missing-var "EnvironmentNameLower"
                             :hint "Run 'source ./pre-build.sh' before running integration tests"})))

        ctx (-> {:service-name :test-service
                 :hosted-zone-name (util/get-env "PublicHostedZoneName" "example.com")
                 :environment-name-lower env-name}
                (aws-ctx/init))

        ;; Validate that AWS context was initialized correctly
        _ (when-not (get-in ctx [:aws :region])
            (throw (ex-info "AWS context initialization failed - missing region"
                            {:aws-config (:aws ctx)
                             :hint "Ensure Region, AWS_DEFAULT_REGION, or AWS_REGION environment variable is set. Run 'source ./pre-build.sh' before running integration tests"})))

        _ (when-not (get-in ctx [:aws :account-id])
            (throw (ex-info "AWS context initialization failed - missing account-id"
                            {:aws-config (:aws ctx)
                             :hint "Ensure AccountId environment variable is set. Run 'source ./pre-build.sh' before running integration tests"})))]
    (binding [*test-ctx* ctx]
      (test-fn))))

(defn cleanup-test-aggregates
  "Clean up test aggregates from S3 after each test.
   This ensures tests don't interfere with each other."
  [test-fn]
  (try
    (test-fn)
    (finally
      ;; Note: S3 cleanup is optional since each test uses unique UUIDs
      ;; and the compliance tests verify isolation
      nil)))

;; Setup S3 context (runs once for all tests)
(use-fixtures :once
  setup-s3-ctx)

;; Clean up between tests and bind dynamic vars (runs before each test)
(use-fixtures :each
  cleanup-test-aggregates
  (fn [test-fn]
    (binding [compliance/*ctx-factory* make-s3-ctx
              compliance/*dal-wrapper* with-s3-dal]
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

;; Category F: Error Handling
(deftest f1-aggregate-without-id-field-must-throw
  (compliance/f1-aggregate-without-id-field-must-throw))

(deftest f2-nil-aggregate-must-throw
  (compliance/f2-nil-aggregate-must-throw))

;; Category G: Concurrent Operations
(deftest g1-multiple-updates-same-aggregate
  (compliance/g1-multiple-updates-same-aggregate))

(deftest g2-multiple-updates-different-aggregates
  (compliance/g2-multiple-updates-different-aggregates))

;; Category H: Version History
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
