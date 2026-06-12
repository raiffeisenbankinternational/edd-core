(ns edd.postgres.event-store-compliance-it
  "Postgres implementation of the event store compliance suite.
   Runs the shared tests from edd.compliance.event-store against a real
   PostgreSQL instance.

   Requires the integration environment (run with: make it):
   - DatabaseEndpoint / DatabasePort / DatabasePassword
   - test and prod realm schemas migrated (done by pre-build.sh)"
  (:require [clojure.test :refer [deftest use-fixtures]]
            [edd.compliance.event-store :as compliance]
            [edd.dal :as dal]
            [edd.postgres.event-store :as event-store]
            [lambda.util :as util]
            [lambda.uuid :as uuid]))

;;; ============================================================================
;;; Test Setup
;;; ============================================================================

(defn make-postgres-ctx
  [& {:keys [realm service-name]
      :or   {realm compliance/test-realm}}]
  (let [base-ctx
        {:service-name (or service-name
                           (keyword (str "compliance-" (uuid/gen))))
         :meta         {:realm realm}
         :db           {:endpoint (util/get-env "DatabaseEndpoint")
                        :port     (util/get-env "DatabasePort" "5432")
                        :name     "postgres"
                        :password (util/get-env "DatabasePassword" "no-secret")}}]
    (event-store/register base-ctx)))

(defn with-postgres-dal
  "Runs the test with the connection pool bound (pool/with-init via dal)."
  [test-fn]
  (dal/with-init (make-postgres-ctx)
    (fn [_ctx]
      (test-fn))))

(use-fixtures :each
  (fn [test-fn]
    (binding [compliance/*ctx-factory* make-postgres-ctx
              compliance/*dal-wrapper* with-postgres-dal]
      (test-fn))))

;;; ============================================================================
;;; Re-export All Compliance Tests
;;; ============================================================================

;; Category A: Basic Event Storage
(deftest a1-store-single-event
  (compliance/a1-store-single-event))

(deftest a2-store-multiple-events-same-aggregate
  (compliance/a2-store-multiple-events-same-aggregate))

(deftest a3-store-events-different-aggregates
  (compliance/a3-store-events-different-aggregates))

(deftest a4-get-events-by-aggregate-id
  (compliance/a4-get-events-by-aggregate-id))

(deftest a5-get-events-with-version-filter
  (compliance/a5-get-events-with-version-filter))

(deftest a6-get-events-with-version-range
  (compliance/a6-get-events-with-version-range))

;; Category B: Identity Management
(deftest b1-store-single-identity
  (compliance/b1-store-single-identity))

(deftest b2-store-multiple-identities-different-aggregates
  (compliance/b2-store-multiple-identities-different-aggregates))

(deftest b3-get-multiple-identities-bulk
  (compliance/b3-get-multiple-identities-bulk))

(deftest b4-identity-not-found
  (compliance/b4-identity-not-found))

(deftest b5-identity-scoped-to-service
  (compliance/b5-identity-scoped-to-service))

;; Category C: Optimistic Locking
(deftest c1-prevent-duplicate-event-seq
  (compliance/c1-prevent-duplicate-event-seq))

(deftest c2-get-max-event-seq
  (compliance/c2-get-max-event-seq))

(deftest c3-get-max-event-seq-no-events
  (compliance/c3-get-max-event-seq-no-events))

;; Category D: Side Effects Tracking
(deftest d1-store-single-effect
  (compliance/d1-store-single-effect))

(deftest d2-store-multiple-effects
  (compliance/d2-store-multiple-effects))

(deftest d3-effect-with-multiple-commands
  (compliance/d3-effect-with-multiple-commands))

(deftest d4-effects-with-breadcrumbs
  (compliance/d4-effects-with-breadcrumbs))

;; Category E: Request Logging
(deftest e1-log-request
  (compliance/e1-log-request))

(deftest e2-log-request-error
  (compliance/e2-log-request-error))

;; Category F: Response Log & Deduplication
(deftest f1-store-results-logs-response-summary
  (compliance/f1-store-results-logs-response-summary))

(deftest f2-same-request-different-breadcrumbs
  (compliance/f2-same-request-different-breadcrumbs))

(deftest f3-no-cached-response-returns-nil
  (compliance/f3-no-cached-response-returns-nil))

(deftest f4-idempotency-key-request-id-plus-breadcrumbs
  (compliance/f4-idempotency-key-request-id-plus-breadcrumbs))

;; Category G: Interaction Traceability
(deftest g1-all-records-share-interaction-id
  (compliance/g1-all-records-share-interaction-id))

(deftest g2-get-records-filters-by-interaction-id
  (compliance/g2-get-records-filters-by-interaction-id))

(deftest g3-cross-service-interaction-tracking
  (compliance/g3-cross-service-interaction-tracking))

(deftest g4-empty-interaction-returns-empty-results
  (compliance/g4-empty-interaction-returns-empty-results))

;; Category H: Atomic Transactions
(deftest h1-store-results-is-atomic
  (compliance/h1-store-results-is-atomic))

(deftest h2-failure-rolls-back-all-changes
  (compliance/h2-failure-rolls-back-all-changes))

(deftest h3-identity-conflict-rolls-back-and-reports-key
  (compliance/h3-identity-conflict-rolls-back-and-reports-key))

(deftest h4-multiple-aggregates-in-transaction
  (compliance/h4-multiple-aggregates-in-transaction))

;; Category I: Edge Cases
(deftest i1-empty-store-results
  (compliance/i1-empty-store-results))

(deftest i2-large-event-payload
  (compliance/i2-large-event-payload))

(deftest i3-special-characters-in-identity
  (compliance/i3-special-characters-in-identity))

(deftest i4-max-event-seq-consistency
  (compliance/i4-max-event-seq-consistency))

(deftest i5-realm-isolation
  (compliance/i5-realm-isolation))
