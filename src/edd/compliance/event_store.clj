(ns edd.compliance.event-store
  "Compliance test suite for event store (DAL) implementations.
   Asserts that all implementations (Memory, Postgres, DynamoDB) honor the
   same contract for store-results, get-events, get-max-event-seq,
   get-aggregate-id-by-identity, get-command-response and get-records.

   This namespace is NOT run directly. Implementation namespaces bind the
   dynamic vars below and re-export each deftest:
   - *ctx-factory*    (fn [& {:keys [realm service-name]}] ctx) — returns a
                      registered ctx for the store under test. Must default
                      service-name to a value unique per call so integration
                      runs against persistent stores stay isolated.
   - *dal-wrapper*    (fn [test-fn]) — store setup/teardown around a test.
   - *read-attempts*  max polls for eventually-consistent reads (DynamoDB
                      GSI queries); 1 means a single read.

   The suite exercises the contract exactly as edd.el.cmd drives it in
   production:
   - store-results receives {:resp {:events :effects :identities :summary}}
   - every request uses a fresh request-id with breadcrumbs [0]
   - effects carry unique breadcrumbs (conj parent-breadcrumb index)
   - conflicting writes throw ExceptionInfo with ex-data
     {:error {:key :concurrent-modification | :identity-conflict}}
     (consumed by edd.el.cmd/retry)
   - store-results is atomic: a conflict persists nothing"
  (:require [clojure.test :refer [deftest is testing]]
            [edd.dal :as dal]
            [lambda.uuid :as uuid]))

;;; ============================================================================
;;; Test Infrastructure
;;; ============================================================================

(def test-realm :test)

(def ^:dynamic *ctx-factory* nil)

(def ^:dynamic *dal-wrapper* nil)

(def ^:dynamic *read-attempts* 1)

(defn make-ctx
  [& {:keys [realm service-name]
      :or   {realm test-realm}}]
  (when-not *ctx-factory*
    (throw (ex-info "No *ctx-factory* bound. Implementation tests must bind this var."
                    {:realm realm})))
  (let [ctx
        (if service-name
          (*ctx-factory* :realm realm :service-name service-name)
          (*ctx-factory* :realm realm))]
    (merge {:request-id     (uuid/gen)
            :interaction-id (uuid/gen)
            :invocation-id  (uuid/gen)
            :breadcrumbs    [0]}
           ctx)))

(defn fresh-request
  "Next request within the same interaction: production assigns a fresh
   request-id per command request, the interaction-id spans them."
  [ctx]
  (assoc ctx
         :request-id (uuid/gen)
         :invocation-id (uuid/gen)
         :breadcrumbs [0]))

(defn make-event
  [& {:keys [event-id id event-seq attrs]
      :or   {event-id  :test-event
             id        (uuid/gen)
             event-seq 1
             attrs     {}}}]
  (merge {:event-id  event-id
          :id        id
          :event-seq event-seq}
         attrs))

(defn make-effect
  [& {:keys [service commands]
      :or   {service  :target-service
             commands [{:cmd-id :test-command
                        :id     (uuid/gen)}]}}]
  {:service  service
   :commands commands})

(defn make-identity
  [& {:keys [identity id]
      :or   {identity (str "identity-" (uuid/gen))
             id       (uuid/gen)}}]
  {:identity identity
   :id       id})

(defn store-results!
  "Stores data through dal/store-results shaped exactly like the production
   flow in edd.el.cmd: summary present, effects given unique breadcrumbs
   (with-breadcrumbs semantics). Events and effects are enriched with the
   request ids so get-records works on every store."
  [ctx {:keys [events effects identities summary]
        :or   {events     []
               effects    []
               identities []
               summary    {:success true}}}]
  (let [enrich
        (fn [item]
          (assoc item
                 :request-id (:request-id ctx)
                 :interaction-id (:interaction-id ctx)))

        effects-with-breadcrumbs
        (vec
         (map-indexed
          (fn [i effect]
            (enrich
             (assoc effect
                    :breadcrumbs
                    (or (:breadcrumbs effect)
                        (conj (:breadcrumbs ctx) i)))))
          effects))

        resp
        (cond-> {:events     (mapv enrich events)
                 :effects    effects-with-breadcrumbs
                 :identities (vec identities)}
          summary (assoc :summary summary))]
    (dal/store-results (assoc ctx :resp resp))))

(defn catch-store-error
  "Runs store-fn, returning the thrown ExceptionInfo or nil on success."
  [store-fn]
  (try
    (store-fn)
    nil
    (catch clojure.lang.ExceptionInfo e
      e)))

(defn error-key
  [e]
  (get-in (ex-data e) [:error :key]))

(defn eventually
  "Re-runs read-fn until pred passes or *read-attempts* is exhausted.
   Returns the last read. Needed for stores with eventually-consistent
   secondary indexes (DynamoDB GSIs back get-records/get-command-response)."
  [pred read-fn]
  (loop [attempt 1]
    (let [result
          (read-fn)]
      (if (or (pred result)
              (>= attempt (long *read-attempts*)))
        result
        (do
          (Thread/sleep 500)
          (recur (inc attempt)))))))

(defn fetch-records
  "get-records for an interaction, polling until the expected counts appear."
  [ctx interaction-id {:keys [events effects]
                       :or   {events  0
                              effects 0}}]
  (eventually
   (fn [records]
     (and (= events (count (:events records)))
          (= effects (count (:effects records)))))
   #(dal/get-records ctx {:interaction-id interaction-id})))

(defn fetch-command-response
  [ctx]
  (eventually some? #(dal/get-command-response ctx)))

(defn fetch-events
  "get-events for an aggregate, polling until the expected count appears."
  [ctx aggregate-id expected-count]
  (eventually
   #(= expected-count (count %))
   #(vec (dal/get-events (assoc ctx :id aggregate-id)))))

(defn fetch-max-event-seq
  [ctx aggregate-id expected]
  (eventually
   #(= expected %)
   #(dal/get-max-event-seq (assoc ctx :id aggregate-id))))

;;; ============================================================================
;;; Validation Helpers
;;; ============================================================================

(def infra-keys
  "Keys the stores may add or carry per-record; not part of the domain data."
  [:request-id :interaction-id :invocation-id :meta])

(defn normalize
  [record]
  (apply dissoc record infra-keys))

(defn sorted-events
  [events]
  (vec
   (sort-by (juxt (comp str :id) :event-seq)
            (map normalize events))))

(defn validate-events-ordered
  "For get-events: order by event-seq is part of the contract."
  [actual expected]
  (is
   (= (mapv normalize expected)
      (mapv normalize actual))))

(defn validate-event-set
  "For get-records: cross-aggregate order is not part of the contract."
  [actual expected]
  (is
   (= (sorted-events expected)
      (sorted-events actual))))

;;; ============================================================================
;;; Category A: Basic Event Storage
;;; ============================================================================

(deftest a1-store-single-event
  (testing "A1: Store a single event and retrieve it via get-records"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             event
             (make-event)

             _
             (store-results! ctx {:events [event]})

             records
             (fetch-records ctx (:interaction-id ctx) {:events 1})]
         (validate-event-set (:events records) [event]))))))

(deftest a2-store-multiple-events-same-aggregate
  (testing "A2: Store multiple events for the same aggregate in sequence"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             events
             [(make-event :id agg-id :event-seq 1 :attrs {:value "first"})
              (make-event :id agg-id :event-seq 2 :attrs {:value "second"})
              (make-event :id agg-id :event-seq 3 :attrs {:value "third"})]

             _
             (store-results! ctx {:events events})

             records
             (fetch-records ctx (:interaction-id ctx) {:events 3})]
         (validate-event-set (:events records) events)

         (validate-events-ordered (fetch-events ctx agg-id 3)
                                  events))))))

(deftest a3-store-events-different-aggregates
  (testing "A3: Store events for different aggregates in the same request"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             events
             [(make-event :id (uuid/gen) :event-seq 1)
              (make-event :id (uuid/gen) :event-seq 1)
              (make-event :id (uuid/gen) :event-seq 1)]

             _
             (store-results! ctx {:events events})

             records
             (fetch-records ctx (:interaction-id ctx) {:events 3})]
         (validate-event-set (:events records) events))))))

(deftest a4-get-events-by-aggregate-id
  (testing "A4: get-events returns only the requested aggregate, ordered"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             target-id
             (uuid/gen)

             other-id
             (uuid/gen)

             target-event-1
             (make-event :id target-id :event-seq 1)

             target-event-2
             (make-event :id target-id :event-seq 2)

             other-event
             (make-event :id other-id :event-seq 1)

             _
             (store-results! ctx {:events [target-event-1
                                           other-event
                                           target-event-2]})

             retrieved
             (fetch-events ctx target-id 2)]
         (is
          (= 2
             (count retrieved)))

         (validate-events-ordered retrieved [target-event-1 target-event-2]))))))

(deftest a5-get-events-with-version-filter
  (testing "A5: get-events with :version returns only newer events"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             events
             (mapv #(make-event :id agg-id :event-seq %)
                   [1 2 3 4])

             _
             (store-results! ctx {:events events})

             _
             (fetch-events ctx agg-id 4)

             retrieved
             (dal/get-events (assoc ctx :id agg-id :version 2))]
         (is
          (= 2
             (count retrieved)))

         (validate-events-ordered retrieved (subvec events 2)))))))

(deftest a6-get-events-with-version-range
  (testing "A6: get-events with [from to] version vector returns the range inclusive"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             events
             (mapv #(make-event :id agg-id :event-seq %)
                   [1 2 3 4 5])

             retrieved
             (do
               (store-results! ctx {:events events})
               (fetch-events ctx agg-id 5)
               (dal/get-events (assoc ctx :id agg-id :version [2 4])))]
         (is
          (= 3
             (count retrieved)))

         (validate-events-ordered retrieved (subvec events 1 4)))))))

;;; ============================================================================
;;; Category B: Identity Management
;;; ============================================================================

(deftest b1-store-single-identity
  (testing "B1: Store a single identity and resolve it"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             identity
             (make-identity :id agg-id)

             _
             (store-results! ctx {:identities [identity]})

             retrieved
             (dal/get-aggregate-id-by-identity
              (assoc ctx :identity (:identity identity)))]
         (is
          (= agg-id
             retrieved)))))))

(deftest b2-store-multiple-identities-different-aggregates
  (testing "B2: Store multiple identities for different aggregates"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             identities
             [(make-identity) (make-identity) (make-identity)]

             _
             (store-results! ctx {:identities identities})]
         (doseq [{:keys [identity id]} identities]
           (is
            (= id
               (dal/get-aggregate-id-by-identity
                (assoc ctx :identity identity))))))))))

(deftest b3-get-multiple-identities-bulk
  (testing "B3: Bulk identity lookup returns {identity -> aggregate-id}"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             identity-1
             (make-identity)

             identity-2
             (make-identity)

             _
             (store-results! ctx {:identities [identity-1 identity-2]})

             retrieved
             (dal/get-aggregate-id-by-identity
              (assoc ctx :identity [(:identity identity-1)
                                    (:identity identity-2)]))]
         (is
          (map? retrieved))

         (is
          (= {(:identity identity-1) (:id identity-1)
              (:identity identity-2) (:id identity-2)}
             retrieved)))))))

(deftest b4-identity-not-found
  (testing "B4: Resolving a non-existent identity returns nil"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             retrieved
             (dal/get-aggregate-id-by-identity
              (assoc ctx :identity (str "missing-" (uuid/gen))))]
         (is
          (nil? retrieved)))))))

(deftest b5-identity-scoped-to-service
  (testing "B5: The same identity maps to different aggregates per service"
    (*dal-wrapper*
     (fn []
       (let [identity-value
             (str "shared-identity-" (uuid/gen))

             ctx-a
             (make-ctx)

             ctx-b
             (make-ctx)

             agg-id-a
             (uuid/gen)

             agg-id-b
             (uuid/gen)

             _
             (store-results! ctx-a
                             {:identities [(make-identity :identity identity-value
                                                          :id agg-id-a)]})

             _
             (store-results! ctx-b
                             {:identities [(make-identity :identity identity-value
                                                          :id agg-id-b)]})]
         (is
          (= agg-id-a
             (dal/get-aggregate-id-by-identity
              (assoc ctx-a :identity identity-value))))

         (is
          (= agg-id-b
             (dal/get-aggregate-id-by-identity
              (assoc ctx-b :identity identity-value)))))))))

;;; ============================================================================
;;; Category C: Optimistic Locking
;;; ============================================================================

(deftest c1-prevent-duplicate-event-seq
  (testing "C1: A second request writing the same (id, event-seq) must throw
            :concurrent-modification and leave the original event untouched"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             original
             (make-event :id agg-id :event-seq 1 :attrs {:value "original"})

             _
             (store-results! ctx {:events [original]})

             _
             (fetch-events ctx agg-id 1)

             error
             (catch-store-error
              #(store-results! (fresh-request ctx)
                               {:events [(make-event :id agg-id
                                                     :event-seq 1
                                                     :attrs {:value "intruder"})]}))]
         (is
          (some? error))

         (is
          (= :concurrent-modification
             (error-key error)))

         (validate-events-ordered (fetch-events ctx agg-id 1)
                                  [original]))))))

(deftest c2-get-max-event-seq
  (testing "C2: get-max-event-seq returns the highest event-seq"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             _
             (store-results! ctx {:events (mapv #(make-event :id agg-id :event-seq %)
                                                [1 2 3])})]
         (is
          (= 3
             (fetch-max-event-seq ctx agg-id 3))))))))

(deftest c3-get-max-event-seq-no-events
  (testing "C3: get-max-event-seq returns 0 for an unknown aggregate"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)]
         (is
          (= 0
             (dal/get-max-event-seq (assoc ctx :id (uuid/gen))))))))))

;;; ============================================================================
;;; Category D: Side Effects Tracking
;;; ============================================================================

(deftest d1-store-single-effect
  (testing "D1: Store a single effect and retrieve it via get-records"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             effect
             (make-effect)

             _
             (store-results! ctx {:effects [effect]})

             records
             (fetch-records ctx (:interaction-id ctx) {:effects 1})

             retrieved
             (first (:effects records))]
         (is
          (= (:service effect)
             (:service retrieved)))

         (is
          (= (:commands effect)
             (:commands retrieved))))))))

(deftest d2-store-multiple-effects
  (testing "D2: Store multiple effects in a single request"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             effects
             [(make-effect :service :service-a)
              (make-effect :service :service-b)]

             _
             (store-results! ctx {:effects effects})

             records
             (fetch-records ctx (:interaction-id ctx) {:effects 2})]
         (is
          (= #{:service-a :service-b}
             (set (map :service (:effects records))))))))))

(deftest d3-effect-with-multiple-commands
  (testing "D3: An effect carries all of its commands (fan-out)"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             commands
             [{:cmd-id :cmd-1 :id (uuid/gen)}
              {:cmd-id :cmd-2 :id (uuid/gen)}
              {:cmd-id :cmd-3 :id (uuid/gen)}]

             _
             (store-results! ctx {:effects [(make-effect :commands commands)]})

             records
             (fetch-records ctx (:interaction-id ctx) {:effects 1})]
         (is
          (= commands
             (:commands (first (:effects records))))))))))

(deftest d4-effects-with-breadcrumbs
  (testing "D4: Effects keep their per-effect breadcrumbs (with-breadcrumbs contract)"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             _
             (store-results! ctx {:effects [(make-effect)
                                            (make-effect)
                                            (make-effect)]})

             records
             (fetch-records ctx (:interaction-id ctx) {:effects 3})]
         (is
          (= #{[0 0] [0 1] [0 2]}
             (set (map :breadcrumbs (:effects records))))))))))

;;; ============================================================================
;;; Category E: Request Logging (write-only API: must not throw)
;;; ============================================================================

(deftest e1-log-request
  (testing "E1: log-request accepts a command request"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             outcome
             (catch-store-error
              #(dal/log-request ctx {:commands    [{:cmd-id :test-cmd
                                                    :id     (uuid/gen)}]
                                     :request-id  (:request-id ctx)
                                     :breadcrumbs [0]}))]
         (is
          (nil? outcome)))))))

(deftest e2-log-request-error
  (testing "E2: log-request-error records an error for a logged request"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             body
             {:commands    [{:cmd-id :test-cmd
                             :id     (uuid/gen)}]
              :request-id  (:request-id ctx)
              :breadcrumbs [0]}

             outcome
             (catch-store-error
              #(do
                 (dal/log-request ctx body)
                 (dal/log-request-error ctx body {:error "Test error"})))]
         (is
          (nil? outcome)))))))

;;; ============================================================================
;;; Category F: Response Log & Deduplication
;;; ============================================================================

(deftest f1-store-results-logs-response-summary
  (testing "F1: store-results with :summary makes it retrievable via
            get-command-response :data — the framework's dedup contract"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             summary
             {:success true
              :events  1
              :effects 0}

             _
             (store-results! ctx {:events  [(make-event)]
                                  :summary summary})

             response
             (fetch-command-response ctx)]
         (is
          (some? response))

         (is
          (= summary
             (:data response))))))))

(deftest f2-same-request-different-breadcrumbs
  (testing "F2: Same request-id with different breadcrumbs are distinct responses"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             summary-1
             {:success true :step 1}

             summary-2
             {:success true :step 2}

             _
             (store-results! ctx
                             {:events  [(make-event)]
                              :summary summary-1})

             _
             (store-results! (assoc ctx :breadcrumbs [0 0])
                             {:events  [(make-event)]
                              :summary summary-2})]
         (is
          (= summary-1
             (:data (fetch-command-response ctx))))

         (is
          (= summary-2
             (:data (fetch-command-response (assoc ctx :breadcrumbs [0 0]))))))))))

(deftest f3-no-cached-response-returns-nil
  (testing "F3: Unknown request-id has no cached response"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)]
         (is
          (nil? (dal/get-command-response ctx))))))))

(deftest f4-idempotency-key-request-id-plus-breadcrumbs
  (testing "F4: The idempotency key is exactly request-id + breadcrumbs"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             _
             (store-results! ctx {:events  [(make-event)]
                                  :summary {:success true}})

             exact-match
             (fetch-command-response ctx)]
         (is
          (some? exact-match))

         (is
          (nil? (dal/get-command-response
                 (assoc ctx :request-id (uuid/gen)))))

         (is
          (nil? (dal/get-command-response
                 (assoc ctx :breadcrumbs [0 1])))))))))

;;; ============================================================================
;;; Category G: Interaction Traceability
;;; ============================================================================

(deftest g1-all-records-share-interaction-id
  (testing "G1: Events and effects of one request share the interaction-id"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             _
             (store-results! ctx {:events  [(make-event) (make-event)]
                                  :effects [(make-effect)]})

             records
             (fetch-records ctx (:interaction-id ctx) {:events  2
                                                       :effects 1})]
         (is
          (= 2
             (count (:events records))))

         (is
          (= 1
             (count (:effects records)))))))))

(deftest g2-get-records-filters-by-interaction-id
  (testing "G2: get-records returns only records of the given interaction"
    (*dal-wrapper*
     (fn []
       (let [ctx-1
             (make-ctx)

             ctx-2
             (assoc (fresh-request ctx-1)
                    :interaction-id (uuid/gen))

             event-1
             (make-event)

             event-2
             (make-event)

             _
             (store-results! ctx-1 {:events [event-1]})

             _
             (store-results! ctx-2 {:events [event-2]})

             records-1
             (fetch-records ctx-1 (:interaction-id ctx-1) {:events 1})

             records-2
             (fetch-records ctx-2 (:interaction-id ctx-2) {:events 1})]
         (validate-event-set (:events records-1) [event-1])

         (validate-event-set (:events records-2) [event-2]))))))

(deftest g3-cross-service-interaction-tracking
  (testing "G3: Effects to different services share the same interaction-id"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             _
             (store-results! ctx {:effects [(make-effect :service :service-a)
                                            (make-effect :service :service-b)
                                            (make-effect :service :service-c)]})

             records
             (fetch-records ctx (:interaction-id ctx) {:effects 3})]
         (is
          (= #{:service-a :service-b :service-c}
             (set (map :service (:effects records))))))))))

(deftest g4-empty-interaction-returns-empty-results
  (testing "G4: An unknown interaction-id yields no records"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             records
             (dal/get-records ctx {:interaction-id (uuid/gen)})]
         (is
          (empty? (:events records)))

         (is
          (empty? (:effects records))))))))

;;; ============================================================================
;;; Category H: Atomic Transactions
;;; ============================================================================

(deftest h1-store-results-is-atomic
  (testing "H1: Events, effects and identities of one request persist together"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             identity
             (make-identity :id agg-id)

             _
             (store-results! ctx {:events     [(make-event :id agg-id)]
                                  :effects    [(make-effect)]
                                  :identities [identity]})

             records
             (fetch-records ctx (:interaction-id ctx) {:events  1
                                                       :effects 1})]
         (is
          (= 1
             (count (:events records))))

         (is
          (= 1
             (count (:effects records))))

         (is
          (= agg-id
             (dal/get-aggregate-id-by-identity
              (assoc ctx :identity (:identity identity))))))))))

(deftest h2-failure-rolls-back-all-changes
  (testing "H2: A conflicting event aborts the WHOLE request — the other
            event, effect, identity and response summary must not persist"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             innocent-agg-id
             (uuid/gen)

             _
             (store-results! ctx {:events [(make-event :id agg-id :event-seq 1)]})

             failing-ctx
             (assoc (fresh-request ctx)
                    :interaction-id (uuid/gen))

             identity
             (make-identity)

             error
             (catch-store-error
              #(store-results! failing-ctx
                               {:events     [(make-event :id innocent-agg-id
                                                         :event-seq 1)
                                             (make-event :id agg-id
                                                         :event-seq 1)]
                                :effects    [(make-effect)]
                                :identities [identity]}))]
         (is
          (some? error))

         (is
          (= :concurrent-modification
             (error-key error)))

         (is
          (empty? (dal/get-events (assoc failing-ctx :id innocent-agg-id))))

         (is
          (nil? (dal/get-aggregate-id-by-identity
                 (assoc failing-ctx :identity (:identity identity)))))

         (is
          (nil? (dal/get-command-response failing-ctx)))

         (let [records
               (dal/get-records failing-ctx
                                {:interaction-id (:interaction-id failing-ctx)})]
           (is
            (empty? (:events records)))

           (is
            (empty? (:effects records)))))))))

(deftest h3-identity-conflict-rolls-back-and-reports-key
  (testing "H3: A duplicate identity aborts the request with :identity-conflict
            and persists nothing — postgres, dynamodb and memory must agree"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             identity-value
             (str "conflicting-" (uuid/gen))

             original-agg-id
             (uuid/gen)

             _
             (store-results! ctx {:identities [(make-identity :identity identity-value
                                                              :id original-agg-id)]})

             failing-ctx
             (assoc (fresh-request ctx)
                    :interaction-id (uuid/gen))

             other-agg-id
             (uuid/gen)

             error
             (catch-store-error
              #(store-results! failing-ctx
                               {:events     [(make-event :id other-agg-id
                                                         :event-seq 1)]
                                :identities [(make-identity :identity identity-value
                                                            :id other-agg-id)]}))]
         (is
          (some? error))

         (is
          (= :identity-conflict
             (error-key error)))

         (is
          (= original-agg-id
             (dal/get-aggregate-id-by-identity
              (assoc ctx :identity identity-value))))

         (is
          (empty? (dal/get-events (assoc failing-ctx :id other-agg-id)))))))))

(deftest h4-multiple-aggregates-in-transaction
  (testing "H4: One request can atomically store events for many aggregates"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-ids
             [(uuid/gen) (uuid/gen) (uuid/gen)]

             _
             (store-results! ctx {:events (mapv #(make-event :id % :event-seq 1)
                                                agg-ids)})

             records
             (fetch-records ctx (:interaction-id ctx) {:events 3})]
         (is
          (= 3
             (count (:events records))))

         (doseq [agg-id agg-ids]
           (is
            (= 1
               (count (fetch-events ctx agg-id 1))))))))))

;;; ============================================================================
;;; Category I: Edge Cases
;;; ============================================================================

(deftest i1-empty-store-results
  (testing "I1: A request with no events/effects/identities still succeeds
            and logs its response summary"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             summary
             {:success true
              :events  0}

             _
             (store-results! ctx {:summary summary})

             records
             (dal/get-records ctx {:interaction-id (:interaction-id ctx)})]
         (is
          (empty? (:events records)))

         (is
          (empty? (:effects records)))

         (is
          (= summary
             (:data (fetch-command-response ctx)))))))))

(deftest i2-large-event-payload
  (testing "I2: A large payload round-trips unchanged"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             payload
             (vec (range 1000))

             _
             (store-results! ctx {:events [(make-event :id agg-id
                                                       :attrs {:data payload})]})

             retrieved
             (fetch-events ctx agg-id 1)]
         (is
          (= 1
             (count retrieved)))

         (is
          (= payload
             (:data (first retrieved)))))))))

(deftest i3-special-characters-in-identity
  (testing "I3: Identities round-trip special characters"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             special-identity
             (str "user+test." (uuid/gen) "@example.com")

             _
             (store-results! ctx {:identities [(make-identity :identity special-identity
                                                              :id agg-id)]})]
         (is
          (= agg-id
             (dal/get-aggregate-id-by-identity
              (assoc ctx :identity special-identity)))))))))

(deftest i4-max-event-seq-consistency
  (testing "I4: get-max-event-seq is consistent across multiple requests"
    (*dal-wrapper*
     (fn []
       (let [ctx
             (make-ctx)

             agg-id
             (uuid/gen)

             batches
             (partition-all 3 (range 1 11))

             _
             (doseq [batch batches]
               (store-results! (assoc (fresh-request ctx)
                                      :interaction-id (uuid/gen))
                               {:events (mapv #(make-event :id agg-id :event-seq %)
                                              batch)}))]
         (is
          (= 10
             (fetch-max-event-seq ctx agg-id 10))))))))

(deftest i5-realm-isolation
  (testing "I5: Records in different realms are invisible to each other.
            Re-export only where multiple realms are provisioned."
    (*dal-wrapper*
     (fn []
       (let [service-name
             (keyword (str "compliance-" (uuid/gen)))

             ctx-test
             (make-ctx :realm :test :service-name service-name)

             ctx-prod
             (assoc (make-ctx :realm :prod :service-name service-name)
                    :interaction-id (:interaction-id ctx-test))

             agg-id
             (uuid/gen)

             identity
             (make-identity :id agg-id)

             _
             (store-results! ctx-test
                             {:events     [(make-event :id agg-id :event-seq 1)]
                              :identities [identity]})

             records-prod
             (dal/get-records ctx-prod
                              {:interaction-id (:interaction-id ctx-prod)})]
         (is
          (= 1
             (count (dal/get-events (assoc ctx-test :id agg-id)))))

         (is
          (empty? (dal/get-events (assoc ctx-prod :id agg-id))))

         (is
          (empty? (:events records-prod)))

         (is
          (nil? (dal/get-aggregate-id-by-identity
                 (assoc ctx-prod :identity (:identity identity)))))

         (is
          (= 0
             (dal/get-max-event-seq (assoc ctx-prod :id agg-id)))))))))
