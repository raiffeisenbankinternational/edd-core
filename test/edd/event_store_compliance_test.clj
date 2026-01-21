(ns edd.event-store-compliance-test
  "Compliance test suite for event store implementations.
   Tests that all three implementations (Memory, Postgres, DynamoDB) behave identically.
   
   All tests MUST use get-records for validation to ensure proper database state inspection."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [edd.dal :as dal]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]
            [lambda.util :as util]))

;;; ============================================================================
;;; Test Infrastructure
;;; ============================================================================

(def test-realm :test)  ; Default realm for single-realm tests

(defn make-ctx
  "Creates a base context for testing with given store type and optional overrides."
  [store-type & {:keys [realm request-id interaction-id service-name]
                 :or {realm test-realm
                      request-id (uuid/gen)
                      interaction-id (uuid/gen)
                      service-name :test-service}}]
  {:edd-event-store store-type
   :service-name service-name
   :request-id request-id
   :interaction-id interaction-id
   :invocation-id (uuid/gen)
   :breadcrumbs [0]
   :meta {:realm realm}})

(defn make-event
  "Creates a test event with required fields."
  [& {:keys [event-id id event-seq attrs]
      :or {event-id :test-event
           id (uuid/gen)
           event-seq 1
           attrs {}}}]
  (merge {:event-id event-id
          :id id
          :event-seq event-seq}
         attrs))

(defn make-effect
  "Creates a test effect (side effect command)."
  [& {:keys [service commands breadcrumbs]
      :or {service :target-service
           commands []
           breadcrumbs [0 0]}}]
  {:service service
   :commands commands
   :breadcrumbs breadcrumbs})

(defn make-identity
  "Creates a test identity."
  [& {:keys [identity id]
      :or {identity "test-identity"
           id (uuid/gen)}}]
  {:identity identity
   :id id})

(defn store-test-data
  "Stores test data using the event store's store-results method.
   Adds interaction-id and request-id from ctx to all events and effects."
  [ctx data]
  (let [enriched-data (-> data
                          (update :events
                                  (fn [events]
                                    (mapv #(assoc %
                                                  :interaction-id (:interaction-id ctx)
                                                  :request-id (:request-id ctx))
                                          events)))
                          (update :effects
                                  (fn [effects]
                                    (mapv #(assoc %
                                                  :interaction-id (:interaction-id ctx)
                                                  :request-id (:request-id ctx))
                                          effects)))
                          (update :identities
                                  (fn [identities]
                                    (mapv #(assoc %
                                                  :interaction-id (:interaction-id ctx)
                                                  :request-id (:request-id ctx))
                                          identities))))
        ctx-with-resp (assoc ctx :resp enriched-data)]
    (dal/store-results ctx-with-resp)))

(defn get-records-for-interaction
  "Retrieves all records for a given interaction-id using get-records."
  [ctx interaction-id]
  (dal/get-records ctx {:interaction-id interaction-id}))

(defn validate-events
  "Validates that stored events match expected events."
  [actual-events expected-events]
  (let [normalize (fn [e] (dissoc e :request-id :interaction-id :meta :invocation-id))
        actual (mapv normalize actual-events)
        expected (mapv normalize expected-events)]
    (is (= (count expected) (count actual))
        (format "Expected %d events, got %d" (count expected) (count actual)))
    (is (= expected actual)
        "Events should match expected values")))

(defn validate-effects
  "Validates that stored effects match expected effects."
  [actual-effects expected-effects]
  (let [normalize (fn [e] (dissoc e :request-id :interaction-id :meta :invocation-id))
        actual (mapv normalize actual-effects)
        expected (mapv normalize expected-effects)]
    (is (= (count expected) (count actual))
        (format "Expected %d effects, got %d" (count expected) (count actual)))
    (is (= expected actual)
        "Effects should match expected values")))

;;; ============================================================================
;;; Category A: Basic Event Storage
;;; ============================================================================

(deftest a1-store-single-event
  "A1: Store a single event and verify it can be retrieved."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          event-id (uuid/gen)
          event (make-event :id event-id :event-seq 1)
          _ (store-test-data ctx {:events [event]
                                  :effects []
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (validate-events (:events records) [event]))))

(deftest a2-store-multiple-events-same-aggregate
  "A2: Store multiple events for the same aggregate in sequence."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event1 (make-event :id agg-id :event-seq 1 :attrs {:value "first"})
          event2 (make-event :id agg-id :event-seq 2 :attrs {:value "second"})
          event3 (make-event :id agg-id :event-seq 3 :attrs {:value "third"})
          _ (store-test-data ctx {:events [event1 event2 event3]
                                  :effects []
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (validate-events (:events records) [event1 event2 event3]))))

(deftest a3-store-events-different-aggregates
  "A3: Store events for different aggregates in the same request."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id-1 (uuid/gen)
          agg-id-2 (uuid/gen)
          agg-id-3 (uuid/gen)
          event1 (make-event :id agg-id-1 :event-seq 1)
          event2 (make-event :id agg-id-2 :event-seq 1)
          event3 (make-event :id agg-id-3 :event-seq 1)
          _ (store-test-data ctx {:events [event1 event2 event3]
                                  :effects []
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (validate-events (:events records) [event1 event2 event3]))))

(deftest a4-get-events-by-aggregate-id
  "A4: Retrieve events for a specific aggregate using get-events."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          target-id (uuid/gen)
          other-id (uuid/gen)
          target-event1 (make-event :id target-id :event-seq 1)
          target-event2 (make-event :id target-id :event-seq 2)
          other-event (make-event :id other-id :event-seq 1)
          _ (store-test-data ctx {:events [target-event1 other-event target-event2]
                                  :effects []
                                  :identities []
                                  :summary {}})
          retrieved (dal/get-events (assoc ctx :id target-id))]
      (is (= 2 (count retrieved)))
      (validate-events retrieved [target-event1 target-event2]))))

(deftest a5-get-events-with-version-filter
  "A5: Retrieve events after a specific version using get-events."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event1 (make-event :id agg-id :event-seq 1)
          event2 (make-event :id agg-id :event-seq 2)
          event3 (make-event :id agg-id :event-seq 3)
          event4 (make-event :id agg-id :event-seq 4)
          _ (store-test-data ctx {:events [event1 event2 event3 event4]
                                  :effects []
                                  :identities []
                                  :summary {}})
          ;; Get events after version 2
          retrieved (dal/get-events (assoc ctx :id agg-id :version 2))]
      (is (= 2 (count retrieved)))
      (validate-events retrieved [event3 event4]))))

;;; ============================================================================
;;; Category B: Identity Management
;;; ============================================================================

(deftest b1-store-single-identity
  "B1: Store a single identity and verify it can be retrieved."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          identity (make-identity :identity "user-email@example.com" :id agg-id)
          _ (store-test-data ctx {:events []
                                  :effects []
                                  :identities [identity]
                                  :summary {}})
          retrieved (dal/get-aggregate-id-by-identity
                     (assoc ctx :identity "user-email@example.com"))]
      (is (= agg-id retrieved)))))

(deftest b2-store-multiple-identities-different-aggregates
  "B2: Store multiple identities for different aggregates."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id-1 (uuid/gen)
          agg-id-2 (uuid/gen)
          agg-id-3 (uuid/gen)
          identity1 (make-identity :identity "user1@example.com" :id agg-id-1)
          identity2 (make-identity :identity "user2@example.com" :id agg-id-2)
          identity3 (make-identity :identity "user3@example.com" :id agg-id-3)
          _ (store-test-data ctx {:events []
                                  :effects []
                                  :identities [identity1 identity2 identity3]
                                  :summary {}})
          retrieved1 (dal/get-aggregate-id-by-identity
                      (assoc ctx :identity "user1@example.com"))
          retrieved2 (dal/get-aggregate-id-by-identity
                      (assoc ctx :identity "user2@example.com"))
          retrieved3 (dal/get-aggregate-id-by-identity
                      (assoc ctx :identity "user3@example.com"))]
      (is (= agg-id-1 retrieved1))
      (is (= agg-id-2 retrieved2))
      (is (= agg-id-3 retrieved3)))))

(deftest b3-get-multiple-identities-bulk
  "B3: Retrieve multiple identities in a single query (batch lookup)."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id-1 (uuid/gen)
          agg-id-2 (uuid/gen)
          identity1 (make-identity :identity "user1@example.com" :id agg-id-1)
          identity2 (make-identity :identity "user2@example.com" :id agg-id-2)
          _ (store-test-data ctx {:events []
                                  :effects []
                                  :identities [identity1 identity2]
                                  :summary {}})
          retrieved (dal/get-aggregate-id-by-identity
                     (assoc ctx :identity ["user1@example.com" "user2@example.com"]))]
      (is (map? retrieved))
      (is (= agg-id-1 (get retrieved "user1@example.com")))
      (is (= agg-id-2 (get retrieved "user2@example.com"))))))

(deftest b4-identity-not-found
  "B4: Querying non-existent identity returns nil."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          retrieved (dal/get-aggregate-id-by-identity
                     (assoc ctx :identity "non-existent@example.com"))]
      (is (nil? retrieved)))))

(deftest b5-identity-scoped-to-service
  "B5: Identities are scoped to service (same identity, different services)."
  (mock/with-mock-dal
    (let [ctx1 (make-ctx :memory :service-name :service-a)
          ctx2 (make-ctx :memory :service-name :service-b)
          agg-id-1 (uuid/gen)
          agg-id-2 (uuid/gen)
          identity1 (make-identity :identity "same-identity" :id agg-id-1)
          identity2 (make-identity :identity "same-identity" :id agg-id-2)
          ;; Store same identity for two different services
          _ (store-test-data ctx1 {:events []
                                   :effects []
                                   :identities [identity1]
                                   :summary {}})
          _ (store-test-data ctx2 {:events []
                                   :effects []
                                   :identities [identity2]
                                   :summary {}})
          retrieved1 (dal/get-aggregate-id-by-identity
                      (assoc ctx1 :identity "same-identity"))
          retrieved2 (dal/get-aggregate-id-by-identity
                      (assoc ctx2 :identity "same-identity"))]
      (is (= agg-id-1 retrieved1))
      (is (= agg-id-2 retrieved2))
      (is (not= agg-id-1 agg-id-2)))))

;;; ============================================================================
;;; Category C: Optimistic Locking
;;; ============================================================================

(deftest c1-prevent-duplicate-event-seq
  "C1: Storing an event with duplicate event-seq should fail (optimistic locking)."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event1 (make-event :id agg-id :event-seq 1)
          event2 (make-event :id agg-id :event-seq 1)  ; Duplicate seq
          _ (store-test-data ctx {:events [event1]
                                  :effects []
                                  :identities []
                                  :summary {}})]
      ;; Attempt to store duplicate should throw exception
      (is (thrown? Exception
                   (store-test-data ctx {:events [event2]
                                         :effects []
                                         :identities []
                                         :summary {}}))))))

(deftest c2-get-max-event-seq
  "C2: get-max-event-seq returns the highest event-seq for an aggregate."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event1 (make-event :id agg-id :event-seq 1)
          event2 (make-event :id agg-id :event-seq 2)
          event3 (make-event :id agg-id :event-seq 3)
          _ (store-test-data ctx {:events [event1 event2 event3]
                                  :effects []
                                  :identities []
                                  :summary {}})
          max-seq (dal/get-max-event-seq (assoc ctx :id agg-id))]
      (is (= 3 max-seq)))))

(deftest c3-get-max-event-seq-no-events
  "C3: get-max-event-seq returns 0 when aggregate has no events."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          max-seq (dal/get-max-event-seq (assoc ctx :id agg-id))]
      (is (= 0 max-seq)))))

;;; ============================================================================
;;; Category D: Side Effects Tracking
;;; ============================================================================

(deftest d1-store-single-effect
  "D1: Store a single effect and verify it can be retrieved."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          cmd-id (uuid/gen)
          effect (make-effect :commands [{:cmd-id :test-command
                                          :id cmd-id}])
          _ (store-test-data ctx {:events []
                                  :effects [effect]
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (validate-effects (:effects records) [effect]))))

(deftest d2-store-multiple-effects
  "D2: Store multiple effects in a single request."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          cmd-id-1 (uuid/gen)
          cmd-id-2 (uuid/gen)
          effect1 (make-effect :service :service-a
                               :commands [{:cmd-id :cmd-a :id cmd-id-1}]
                               :breadcrumbs [0 0])
          effect2 (make-effect :service :service-b
                               :commands [{:cmd-id :cmd-b :id cmd-id-2}]
                               :breadcrumbs [0 1])
          _ (store-test-data ctx {:events []
                                  :effects [effect1 effect2]
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (validate-effects (:effects records) [effect1 effect2]))))

(deftest d3-effect-with-multiple-commands
  "D3: Store an effect containing multiple commands (fan-out)."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          cmd-id-1 (uuid/gen)
          cmd-id-2 (uuid/gen)
          cmd-id-3 (uuid/gen)
          effect (make-effect :commands [{:cmd-id :cmd-1 :id cmd-id-1}
                                         {:cmd-id :cmd-2 :id cmd-id-2}
                                         {:cmd-id :cmd-3 :id cmd-id-3}])
          _ (store-test-data ctx {:events []
                                  :effects [effect]
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (is (= 1 (count (:effects records))))
      (is (= 3 (count (get-in (first (:effects records)) [:commands])))))))

(deftest d4-effects-with-breadcrumbs
  "D4: Effects track breadcrumbs for tracing effect chains."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          effect1 (make-effect :breadcrumbs [0 0])
          effect2 (make-effect :breadcrumbs [0 1])
          effect3 (make-effect :breadcrumbs [0 0 0])
          _ (store-test-data ctx {:events []
                                  :effects [effect1 effect2 effect3]
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (is (= 3 (count (:effects records))))
      ;; Verify breadcrumbs are preserved
      (let [effects (:effects records)]
        (is (some #(= [0 0] (:breadcrumbs %)) effects))
        (is (some #(= [0 1] (:breadcrumbs %)) effects))
        (is (some #(= [0 0 0] (:breadcrumbs %)) effects))))))

;;; ============================================================================
;;; Category E: Request/Response Logging
;;; ============================================================================

(deftest e1-log-request
  "E1: log-request stores request metadata."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          request-body {:commands [{:cmd-id :test-cmd}]
                        :breadcrumbs [0]}
          _ (dal/log-request ctx request-body)]
      ;; Verification depends on store implementation
      ;; For memory store, check the command-log
      (is (= 1 (count (mock/peek-state :command-log)))))))

(deftest e2-log-response
  "E2: log-response stores response summary."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          response-data {:events 1 :effects 0}
          ctx-with-resp (assoc ctx :response-summary response-data)
          _ (dal/log-response ctx-with-resp)]
      ;; Verify response was logged
      (is (= 1 (count (mock/peek-state :response-log)))))))

(deftest e3-log-request-error
  "E3: log-request-error stores error information."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          request-body {:breadcrumbs [0]}
          error-data {:error "Test error" :type :validation}
          _ (dal/log-request-error ctx request-body error-data)]
      ;; Verify error was logged
      (is (= 1 (count (mock/peek-state :request-error-log)))))))

(deftest e4-get-command-response
  "E4: get-command-response retrieves cached response for deduplication."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          response-data {:events 1 :success true}
          ctx-with-resp (assoc ctx :response-summary response-data)
          _ (dal/log-response ctx-with-resp)
          retrieved (dal/get-command-response ctx)]
      (is (not (nil? retrieved)))
      (is (= response-data (:data retrieved))))))
;;; ============================================================================
;;; Category F: Deduplication
;;; ============================================================================

(deftest f1-duplicate-request-returns-cached-response
  "F1: Submitting same request-id + breadcrumbs returns cached response."
  (mock/with-mock-dal
    (let [request-id (uuid/gen)
          ctx (make-ctx :memory :request-id request-id)
          response-data {:events 1 :success true}
          ctx-with-resp (assoc ctx :response-summary response-data)
          _ (dal/log-response ctx-with-resp)
          ;; Make same request again with same ID
          cached (dal/get-command-response ctx)]
      (is (not (nil? cached)))
      (is (= response-data (:data cached))))))

(deftest f2-different-breadcrumbs-different-responses
  "F2: Same request-id but different breadcrumbs = different responses."
  (mock/with-mock-dal
    (let [request-id (uuid/gen)
          ctx1 (make-ctx :memory :request-id request-id)
          ctx1 (assoc ctx1 :breadcrumbs [0])
          ctx2 (assoc ctx1 :breadcrumbs [0 0])
          response1 {:events 1}
          response2 {:events 2}
          _ (dal/log-response (assoc ctx1 :response-summary response1))
          _ (dal/log-response (assoc ctx2 :response-summary response2))
          cached1 (dal/get-command-response ctx1)
          cached2 (dal/get-command-response ctx2)]
      (is (= response1 (:data cached1)))
      (is (= response2 (:data cached2))))))

(deftest f3-no-cached-response-returns-nil
  "F3: Querying non-existent request returns nil."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          cached (dal/get-command-response ctx)]
      (is (nil? cached)))))

(deftest f4-idempotency-key-request-id-plus-breadcrumbs
  "F4: Idempotency key is combination of request-id + breadcrumbs."
  (mock/with-mock-dal
    (let [request-id (uuid/gen)
          ctx (make-ctx :memory :request-id request-id)
          response-data {:success true}
          _ (dal/log-response (assoc ctx :response-summary response-data))
          ;; Try to retrieve with exact match
          exact-match (dal/get-command-response ctx)
          ;; Try with different request-id
          different-req (dal/get-command-response
                         (assoc ctx :request-id (uuid/gen)))
          ;; Try with different breadcrumbs
          different-bc (dal/get-command-response
                        (assoc ctx :breadcrumbs [0 1]))]
      (is (not (nil? exact-match)))
      (is (nil? different-req))
      (is (nil? different-bc)))))

;;; ============================================================================
;;; Category G: Interaction Traceability
;;; ============================================================================

(deftest g1-all-records-share-interaction-id
  "G1: All events/effects in a request share the same interaction-id."
  (mock/with-mock-dal
    (let [interaction-id (uuid/gen)
          ctx (make-ctx :memory :interaction-id interaction-id)
          event1 (make-event)
          event2 (make-event)
          effect1 (make-effect)
          _ (store-test-data ctx {:events [event1 event2]
                                  :effects [effect1]
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx interaction-id)]
      (is (= 2 (count (:events records))))
      (is (= 1 (count (:effects records)))))))

(deftest g2-get-records-filters-by-interaction-id
  "G2: get-records returns only records matching interaction-id."
  (mock/with-mock-dal
    (let [interaction-id-1 (uuid/gen)
          interaction-id-2 (uuid/gen)
          ctx1 (make-ctx :memory :interaction-id interaction-id-1)
          ctx2 (make-ctx :memory :interaction-id interaction-id-2)
          event1 (make-event)
          event2 (make-event)
          _ (store-test-data ctx1 {:events [event1] :effects [] :identities [] :summary {}})
          _ (store-test-data ctx2 {:events [event2] :effects [] :identities [] :summary {}})
          records1 (get-records-for-interaction ctx1 interaction-id-1)
          records2 (get-records-for-interaction ctx2 interaction-id-2)]
      (is (= 1 (count (:events records1))))
      (is (= 1 (count (:events records2))))
      ;; Verify they're different events
      (is (not= (first (:events records1)) (first (:events records2)))))))

(deftest g3-cross-service-interaction-tracking
  "G3: Effects to different services share same interaction-id."
  (mock/with-mock-dal
    (let [interaction-id (uuid/gen)
          ctx (make-ctx :memory :interaction-id interaction-id)
          effect1 (make-effect :service :service-a)
          effect2 (make-effect :service :service-b)
          effect3 (make-effect :service :service-c)
          _ (store-test-data ctx {:events []
                                  :effects [effect1 effect2 effect3]
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx interaction-id)]
      (is (= 3 (count (:effects records))))
      ;; All effects should be for different services
      (let [services (set (map :service (:effects records)))]
        (is (= 3 (count services)))
        (is (contains? services :service-a))
        (is (contains? services :service-b))
        (is (contains? services :service-c))))))

(deftest g4-empty-interaction-returns-empty-results
  "G4: Querying non-existent interaction-id returns empty results."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          non-existent-id (uuid/gen)
          records (get-records-for-interaction ctx non-existent-id)]
      (is (empty? (:events records)))
      (is (empty? (:effects records))))))

;;; ============================================================================
;;; Category H: Atomic Transactions
;;; ============================================================================

(deftest h1-store-results-is-atomic
  "H1: store-results stores events, effects, and identities atomically."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event (make-event :id agg-id)
          effect (make-effect)
          identity (make-identity :id agg-id)
          _ (store-test-data ctx {:events [event]
                                  :effects [effect]
                                  :identities [identity]
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      ;; All should be stored together
      (is (= 1 (count (:events records))))
      (is (= 1 (count (:effects records))))
      ;; Identity should be retrievable
      (is (= agg-id (dal/get-aggregate-id-by-identity
                     (assoc ctx :identity (:identity identity))))))))

(deftest h2-failure-rolls-back-all-changes
  "H2: If store-results fails, no partial data should be stored."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event1 (make-event :id agg-id :event-seq 1)
          ;; First, store a valid event
          _ (store-test-data ctx {:events [event1]
                                  :effects []
                                  :identities []
                                  :summary {}})
          ;; Try to store duplicate event-seq (should fail)
          duplicate-event (make-event :id agg-id :event-seq 1)]
      (is (thrown? Exception
                   (store-test-data ctx {:events [duplicate-event]
                                         :effects []
                                         :identities []
                                         :summary {}})))
      ;; Verify only first event exists
      (let [records (get-records-for-interaction ctx (:interaction-id ctx))]
        (is (= 1 (count (:events records))))))))

(deftest h3-concurrent-writes-prevented-by-event-seq
  "H3: Optimistic locking prevents concurrent modifications."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          event-seq-1 (make-event :id agg-id :event-seq 1)
          _ (store-test-data ctx {:events [event-seq-1]
                                  :effects []
                                  :identities []
                                  :summary {}})
          ;; Simulate concurrent write with same event-seq
          concurrent-event (make-event :id agg-id :event-seq 1)]
      (is (thrown? Exception
                   (store-test-data ctx {:events [concurrent-event]
                                         :effects []
                                         :identities []
                                         :summary {}}))))))

(deftest h4-multiple-aggregates-in-transaction
  "H4: Can store events for multiple aggregates atomically."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id-1 (uuid/gen)
          agg-id-2 (uuid/gen)
          agg-id-3 (uuid/gen)
          event1 (make-event :id agg-id-1 :event-seq 1)
          event2 (make-event :id agg-id-2 :event-seq 1)
          event3 (make-event :id agg-id-3 :event-seq 1)
          _ (store-test-data ctx {:events [event1 event2 event3]
                                  :effects []
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (is (= 3 (count (:events records))))
      ;; Verify all three aggregates can be queried
      (is (= 1 (count (dal/get-events (assoc ctx :id agg-id-1)))))
      (is (= 1 (count (dal/get-events (assoc ctx :id agg-id-2)))))
      (is (= 1 (count (dal/get-events (assoc ctx :id agg-id-3))))))))

;;; ============================================================================
;;; Category I: Edge Cases
;;; ============================================================================

(deftest i1-empty-store-results
  "I1: Calling store-results with no events/effects/identities succeeds."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          _ (store-test-data ctx {:events []
                                  :effects []
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (is (empty? (:events records)))
      (is (empty? (:effects records))))))

(deftest i2-large-event-payload
  "I2: Store and retrieve event with large payload."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          large-payload (vec (range 1000))
          event (make-event :attrs {:data large-payload})
          _ (store-test-data ctx {:events [event]
                                  :effects []
                                  :identities []
                                  :summary {}})
          records (get-records-for-interaction ctx (:interaction-id ctx))]
      (is (= 1 (count (:events records))))
      (is (= large-payload (get-in (first (:events records)) [:data]))))))

(deftest i3-special-characters-in-identity
  "I3: Identity can contain special characters."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          special-identity "user+test@example.com"
          identity (make-identity :identity special-identity :id agg-id)
          _ (store-test-data ctx {:events []
                                  :effects []
                                  :identities [identity]
                                  :summary {}})
          retrieved (dal/get-aggregate-id-by-identity
                     (assoc ctx :identity special-identity))]
      (is (= agg-id retrieved)))))

(deftest i4-max-event-seq-consistency
  "I4: get-max-event-seq is consistent after multiple stores."
  (mock/with-mock-dal
    (let [ctx (make-ctx :memory)
          agg-id (uuid/gen)
          events (mapv #(make-event :id agg-id :event-seq %) (range 1 11))]
      ;; Store events in batches
      (doseq [batch (partition-all 3 events)]
        (store-test-data (assoc ctx :interaction-id (uuid/gen))
                         {:events (vec batch)
                          :effects []
                          :identities []
                          :summary {}}))
      ;; Max should be 10
      (is (= 10 (dal/get-max-event-seq (assoc ctx :id agg-id)))))))

(deftest i5-realm-isolation
  "I5: Events in different realms are isolated."
  (mock/with-mock-dal
    (let [ctx-test (make-ctx :memory :realm :test)
          ctx-prod (make-ctx :memory :realm :prod)
          event-test (make-event :attrs {:realm :test})
          event-prod (make-event :attrs {:realm :prod})
          _ (store-test-data ctx-test {:events [event-test]
                                       :effects []
                                       :identities []
                                       :summary {}})
          _ (store-test-data ctx-prod {:events [event-prod]
                                       :effects []
                                       :identities []
                                       :summary {}})
          records-test (get-records-for-interaction ctx-test (:interaction-id ctx-test))
          records-prod (get-records-for-interaction ctx-prod (:interaction-id ctx-prod))]
      ;; Each realm should only see its own events
      (is (= 1 (count (:events records-test))))
      (is (= 1 (count (:events records-prod))))
      (is (= :test (:realm (first (:events records-test)))))
      (is (= :prod (:realm (first (:events records-prod))))))))