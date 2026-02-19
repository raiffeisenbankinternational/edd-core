;; Test fixture for in-memory Data Access Layer.
;; Provides helpers for writing unit tests with mock data stores.
;;
;; All stores are partitioned by realm (from ctx [:meta :realm], defaults to :test).
;; Helper functions provide normalized views with infrastructure metadata removed.
;;
;; Example:
;;
;; (with-mock-dal
;;   {:event-store [{:id #uuid"..." :event-id :existing}]}
;;   
;;   (handle-cmd ctx {:cmd-id :create-order
;;                    :id (uuid/gen)
;;                    :attrs {...}})
;;   
;;   (verify-state :event-store [{:event-id :order-created ...}]))

(ns edd.test.fixture.dal
  (:require [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
            [edd.el.event :as event]
            [clojure.test :refer [is]]
            [edd.el.cmd :as cmd]
            [edd.response.cache :as response-cache]
            [edd.core :as edd]
            [lambda.util :as util]
            [edd.common :as common]
            [lambda.uuid :as uuid]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.client :as client]
            [lambda.test.fixture.state :refer [*dal-state* *queues*]]
            [lambda.request :as request]
            [edd.el.query :as query]
            [aws.aws :as aws]
            [lambda.ctx :as lambda-ctx]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Data Access Layer ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- like-cond
  "Returns function which checks if map contains key value pair
  described in condition, where value is not exact match"
  [condition]
  (log/info "like-cond" condition)
  (let [k (key (first condition))
        v (val (first condition))]
    (fn [x] (> (.indexOf (get x k) v) 0))))

(defn- equal-cond
  "Returns function which checks if map contains key value pair
  described in condition, where value is exact match"
  [condition]
  (log/info "equal-cond" condition)
  (let [k (key (first condition))
        v (val (first condition))]
    (fn [x] (= (get x k) v))))

(defn- full-search-cond
  "Returns function which checks if map contains any value which contains
  condition"
  [condition]
  (log/info "full-search-cond" condition)
  (fn [x] (some #(> (.indexOf % condition) 0) (vals x))))

(defn map-conditions [condition]
  (cond
    (:like condition) (like-cond (:like condition))
    (:equal condition) (equal-cond (:equal condition))
    (:search condition) (full-search-cond (:search condition))
    :else (fn [_] false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   Test Fixtures   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def default-db
  {:realms {}})

(defn get-realm-from-state
  []
  (get-in @*dal-state* [:realm] :test))

(defn get-store
  [store-key]
  (get-in @*dal-state* [:realms (get-realm-from-state) store-key] []))

(def root-meta-fields
  "Root-level keys stripped by normalize-item when :keep-meta is nil/false.
   Available for test assertions."
  #{:meta :breadcrumbs :invocation-id :request-id :interaction-id :service-name})

(def meta-meta-fields
  "Keys within the :meta sub-map that are infrastructure.
   Stripped by normalize-event; not domain data."
  #{:realm})

(def ^:private strippable-keys root-meta-fields)

(defn- normalize-event
  "Strips infrastructure metadata from an event for clean test assertions.
   Removes :request-id, :interaction-id, :service-name, :breadcrumbs, :invocation-id.
   Within :meta, removes only infrastructure keys (e.g. :realm).
   If :meta becomes empty after stripping, removes it entirely.
   This preserves domain-level :meta entries like {:user ...}."
  [event]
  (let [cleaned (apply dissoc event (disj strippable-keys :meta))
        m (:meta cleaned)]
    (if m
      (let [m' (apply dissoc m meta-meta-fields)]
        (if (seq m')
          (assoc cleaned :meta m')
          (dissoc cleaned :meta)))
      cleaned)))

(defn normalize-item
  "Strips infrastructure metadata from a store item for clean test assertions.

   keep-meta controls which metadata is preserved:
     false/nil  — removes :meta, :breadcrumbs, :invocation-id, :request-id,
                   :interaction-id, :service-name.
     true       — removes nothing.
     [keys]     — keeps listed keys either from root level or within :meta.
                   Everything in strippable-keys not listed is removed.
                   For :meta sub-map: keeps only entries whose key is in the
                   vector. If none of :meta's entries match, :meta is omitted."
  [item keep-meta]
  (cond
    ;; true — keep everything
    (true? keep-meta)
    item

    ;; vector — selective keep
    (vector? keep-meta)
    (let [keep-set   (set keep-meta)
          orig-meta  (:meta item)
          ;; strip all strippable root keys (including :meta for now)
          cleaned    (apply dissoc item strippable-keys)
          ;; add back root-level strippable keys that are in keep-set (except :meta)
          cleaned    (reduce (fn [m k]
                               (if (and (contains? keep-set k)
                                        (contains? item k))
                                 (assoc m k (get item k))
                                 m))
                             cleaned
                             (disj strippable-keys :meta))
          ;; handle :meta sub-map: select-keys for entries in keep-set
          selected   (when orig-meta (select-keys orig-meta keep-set))
          cleaned    (if (seq selected)
                       (assoc cleaned :meta selected)
                       cleaned)]
      cleaned)

    ;; false/nil/default — strip all
    :else
    (apply dissoc item strippable-keys)))

(defn dal-state-accessor
  "Returns realm-scoped store data with normalization for test assertions.

   Strips infrastructure metadata from items using normalize-item.
   Normalization is controlled by :keep-meta in state (see normalize-item).
   For event-store when keep-meta is false: uses normalize-event which preserves
   domain-level :meta (e.g. {:user ...}) but strips infrastructure :meta keys
   (e.g. :realm) and all strippable root keys.
   For event-store when keep-meta is true: keeps everything raw.
   Sorts event-store by event-seq."
  [state key]
  (if (#{:event-store :identity-store :command-store :aggregate-store :aggregate-history
         :response-log :command-log :request-error-log} key)
    (let [realm (get state :realm :test)
          data (get-in state [:realms realm key] [])
          keep-meta (get state :keep-meta false)
          normalize #(normalize-item % keep-meta)]
      (cond
        ;; Event store with default normalization:
        ;; strip infra keys + realm from meta, keep domain meta
        (and (= key :event-store) (not keep-meta))
        (mapv normalize-event (sort-by :event-seq data))

        ;; Event store with keep-meta: use normalize-item
        ;; which respects keep-meta true/vector semantics
        (= key :event-store)
        (mapv normalize (sort-by :event-seq data))

        :else
        (mapv normalize data)))
    (get state key)))

(defn create-identity
  [& [id]]
  (get-in @*dal-state* [:identities (keyword id)] (uuid/gen)))

(defn prepare-dps-calls
  [ctx]
  (mapv
   (fn [%]
     (let [req {:query
                (:query %)}
           req-1 (if (:request-id %)
                   (assoc req :request-id (:request-id %))
                   req)
           req-2 (if (:interaction-id %)
                   (assoc req-1 :interaction-id (:interaction-id %))
                   req-1)]
       (-> {:post (query/calc-service-query-url
                   (:service %)
                   ;; Use meta from dep config, or fall back to ctx meta
                   (or (:meta %) (:meta ctx)))
            :body (util/to-json {:result (:resp %)})
            :req  req-2})))

   (get @*dal-state* :dps
        (get @*dal-state* :deps []))))

(defn aws-get-token
  [_ctx]
  "#mock-id-token")

(def ctx
  (-> {:response-schema-validation :throw-on-error
       :service-name (or (lambda-ctx/get-service-name {}) :local-test)
       :hosted-zone-name "example.com"
       :environment-name-lower "local"
       :meta {:realm :test}}
      (response-cache/register-default)
      (view-store/register)
      (event-store/register)))

(defn create-realm-partitioned-state
  "Partitions flat test state into realm structure.
   Moves top-level store keys into [:realms <realm> <store-key>].
   Enriches identity-store records with :service-name from ctx when missing."
  [base-state service-name]
  (let [realm (get base-state :realm :test)
        store-keys [:event-store :identity-store :command-store :aggregate-store :aggregate-history
                    :response-log :command-log :request-error-log]
        top-level-stores (select-keys base-state store-keys)
        top-level-stores (if (and service-name (:identity-store top-level-stores))
                           (update top-level-stores :identity-store
                                   (fn [ids]
                                     (mapv #(if (:service-name %)
                                              %
                                              (assoc % :service-name service-name))
                                           ids)))
                           top-level-stores)
        existing-realm-stores (get-in base-state [:realms realm] {})
        merged-realm-stores (merge existing-realm-stores top-level-stores)
        clean-base-state (apply dissoc base-state store-keys)]
    (assoc clean-base-state
           :realms {realm merged-realm-stores})))

(defmacro with-mock-dal [& body]
  `(edd/with-stores
     ctx
     #(let [base-state# (merge
                         {:realm :test}  ; Default realm for tests
                         (util/fix-keys
                          ~(if (map? (first body))
                             (merge
                              default-db
                              (dissoc (first body) :seed))
                             default-db)))]
        (binding [*dal-state* (atom (create-realm-partitioned-state base-state# (lambda-ctx/get-service-name ctx)))
                  *queues* {:command-queue (atom [])
                            :seed          ~(if (and (map? (first body)) (:seed (first body)))
                                              (:seed (first body))
                                              '(rand-int 10000000))}
                  util/*cache* (atom {})
                  request/*request* (atom {})]
          %
          (client/mock-http
           {:responses (vec (concat (prepare-dps-calls ctx)
                                    (get @*dal-state* :responses [])))
            :config {:reuse-responses true}}
           (with-redefs
            [aws/get-token aws-get-token
             common/create-identity create-identity]
             (do (log/info "with-mock-dal using seed" (:seed *queues*))
                 ~@body)))))))

(defmacro verify-state
  "Verifies normalized store contents in current realm.
   
   Usage:
     (verify-state :event-store [{:event-id :foo :id ...}])
     (verify-state [{:event-id :foo}] :event-store)"
  [x & [y]]
  `(let [x# ~x
         y# ~y
         [expected# actual#] (if (keyword? y#)
                               [x# (into [] (dal-state-accessor @*dal-state* y#))]
                               [y# (into [] (dal-state-accessor @*dal-state* x#))])]
     (is (= expected# actual#))))

(defmacro verify-state-fn [x fn y]
  `(let [expected# ~y
         actual# (mapv ~fn (dal-state-accessor @*dal-state* ~x))]
     (is (= expected# actual#))))

(defn- pop-state-raw
  "Retrieves and clears realm-scoped store without normalization.
   Used internally when commands need to be re-executed (execute-fx)."
  [store-key]
  (let [realm (get-realm-from-state)
        current-state (get-in @*dal-state* [:realms realm store-key] [])]
    (swap! *dal-state*
           #(assoc-in % [:realms realm store-key] []))
    current-state))

(defn pop-state
  "Retrieves and clears realm-scoped store.
   Returns normalized items (same as peek-state / verify-state).
   For event-store with default normalization: preserves domain-level :meta.
   See normalize-item for :keep-meta semantics."
  [store-key]
  (let [state @*dal-state*
        realm (get state :realm :test)
        keep-meta (get state :keep-meta false)
        data (get-in state [:realms realm store-key] [])
        normalize #(normalize-item % keep-meta)
        normalized (cond
                     (and (= store-key :event-store) (not keep-meta))
                     (mapv normalize-event (sort-by :event-seq data))

                     (= store-key :event-store)
                     (mapv normalize (sort-by :event-seq data))

                     :else
                     (mapv normalize data))]
    (swap! *dal-state*
           #(assoc-in % [:realms realm store-key] []))
    normalized))

(defn peek-state
  "Retrieves normalized realm-scoped store data without removing it.
   Returns normalized items (see normalize-item for :keep-meta semantics).

   With store-key: (peek-state :event-store)
   Without args: (peek-state) returns map of all stores"
  [& x]
  (if x
    (let [store-key (first x)
          state @*dal-state*]
      (dal-state-accessor state store-key))
    (let [state @*dal-state*]
      {:aggregate-store (dal-state-accessor state :aggregate-store)
       :command-store (dal-state-accessor state :command-store)
       :event-store (dal-state-accessor state :event-store)
       :identity-store (dal-state-accessor state :identity-store)})))

(defn- re-parse
  [cmd]
  (util/fix-keys cmd))

(defn handle-cmd
  "Executes a command through the mock DAL.

   By default returns a summarized response (event count, effect count, etc.).
   Options (set on ctx):
     :no-summary  true  — returns full event/effect maps, normalized via
                          normalize-item (strips infra metadata).
     :include-meta true — returns raw response with no normalization at all."
  [{:keys [include-meta no-summary] :as ctx} {:keys [request-id
                                                     interaction-id]
                                              :as cmd}]
  (try
    (let [ctx (cond-> ctx
                request-id (assoc :request-id request-id)
                interaction-id (assoc :interaction-id interaction-id))
          resp (if (contains? cmd :commands)
                 (when (or (= (:service-name ctx)
                              (:service cmd))
                           (= nil
                              (:service cmd)))
                   (cmd/handle-commands ctx
                                        (re-parse cmd)))
                 (cmd/handle-commands ctx
                                      {:commands [(re-parse cmd)]}))]

      (if include-meta
        resp
        (if no-summary
          (let [keep-meta (get @*dal-state* :keep-meta false)
                norm #(normalize-item % keep-meta)]
            (-> resp
                (update :events #(mapv norm %))
                (update :effects #(mapv norm %))))
          resp)))
    (catch Exception ex
      (log/error "CMD execution ERROR" ex)
      (ex-data ex))))

(defn get-commands-response
  [ctx cmd]
  (handle-cmd (assoc ctx
                     :no-summary true)
              cmd))

(defn apply-cmd [ctx cmd]
  (log/info "apply-cmd" cmd)
  (let [resp (handle-cmd (assoc ctx
                                :no-summary true) cmd)]
    (log/debug "apply-cmd returned" (with-out-str (pprint/pprint resp)))
    (tap> resp)
    (doseq [id (distinct (map :id (:events resp)))]
      (event/handle-event (assoc ctx
                                 :apply {:aggregate-id id
                                         :meta         (:meta ctx {})})))
    resp))

(defn- local-service-cmd?
  "Returns true when cmd targets the current service (or has no service set)."
  [ctx cmd]
  (let [svc (:service cmd)]
    (or (nil? svc)
        (= svc (:service-name ctx)))))

(defn- put-back-commands!
  "Puts remote commands back into the command store.
   Used by execute-fx / execute-fx-apply to retain commands
   targeting other services."
  [commands]
  (let [realm (get-realm-from-state)]
    (swap! *dal-state*
           (fn [state]
             (update-in state [:realms realm :command-store]
                        (fn [v] (into (or v []) commands)))))))

(defn execute-fx [ctx]
  (let [all-cmds (pop-state-raw :command-store)
        {local true remote false}
        (group-by #(local-service-cmd? ctx %) all-cmds)]
    (when (seq remote)
      (put-back-commands! remote))
    (doall
     (for [cmd local]
       (handle-cmd ctx cmd)))))

(defn execute-fx-apply [ctx]
  (let [all-cmds (pop-state-raw :command-store)
        {local true remote false}
        (group-by #(local-service-cmd? ctx %) all-cmds)]
    (when (seq remote)
      (put-back-commands! remote))
    (doall
     (for [{:keys [commands] :as cmd} local]
       (doall
        (for [cmd commands]
          (apply-cmd ctx cmd)))))))

(defn execute-fx-apply-all
  "Executes all the side effects until the command store is empty"
  [ctx]
  (while (seq (filter (partial local-service-cmd? ctx)
                      (get-store :command-store)))
    (execute-fx-apply ctx)))

(defn execute-cmd
  "Executes a command and applies all the side effects
   and executes also the produced commands until the
  command store is empty."
  [ctx cmd]
  (apply-cmd ctx cmd)
  (execute-fx-apply-all ctx))

(defn apply-events
  [ctx id]
  (event/handle-event (assoc ctx :apply {:aggregate-id id})))

(defn query
  [ctx query]
  (if (contains? query :query)
    (query/handle-query ctx (re-parse query))
    (query/handle-query ctx (re-parse {:query query}))))

(defn get-by-id
  "Retrieves aggregate by ID using event sourcing (replays events).
   
   Usage:
     (mock/get-by-id ctx {:id #uuid \"...\"})
     (mock/get-by-id ctx #uuid \"...\")
   
   Returns the aggregate map or nil if not found."
  [ctx id-or-query]
  (let [id (if (uuid? id-or-query)
             id-or-query
             (:id id-or-query))]
    (common/get-by-id ctx {:id id})))
