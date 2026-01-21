;; In-memory implementation of event store and command logging
;; Used for unit testing - provides fast, in-memory event sourcing with full DAL support
;;
;; REALM ARCHITECTURE:
;; All stores partitioned by realm (from ctx [:meta :realm], defaults to :test).
;; Structure: {:realms {<realm> {:event-store [...] :command-store [...] ...}}}
;;
;; STORES (all realm-scoped):
;; - :event-store - Immutable event log
;; - :command-store - Side-effect commands (reg-event-fx outputs)
;; - :identity-store - Identity-to-aggregate-ID mappings
;; - :command-log - Incoming command requests
;; - :response-log - Command responses (for idempotency)
;; - :request-error-log - Failed requests
;; - :aggregate-store - Materialized views (managed by view-store)

(ns edd.memory.event-store
  (:require
   [clojure.tools.logging :as log]
   [lambda.test.fixture.state :refer [*dal-state* *queues*]]
   [edd.dal :refer [with-init
                    get-events
                    get-max-event-seq
                    get-aggregate-id-by-identity
                    get-command-response
                    log-request
                    log-request-error
                    log-response
                    get-records
                    store-results]]
   [lambda.util :as util]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn get-realm
  "Extract realm from context, defaults to :test"
  [ctx]
  (get-in ctx [:meta :realm] :test))

(defn get-realm-store
  "Get realm-scoped store from *dal-state*"
  [ctx store-key]
  (let [realm (get-realm ctx)]
    (get-in @*dal-state* [:realms realm store-key] [])))

(defn update-realm-store!
  "Update realm-scoped store in *dal-state* atomically"
  [ctx store-key update-fn]
  (let [realm (get-realm ctx)]
    (swap! *dal-state*
           (fn [state]
             (update-in state [:realms realm store-key] update-fn)))))

(defn fix-keys
  [val]
  (-> val
      (util/to-json)
      (util/to-edn)))

(defn store-identity
  [ctx identity]
  (util/d-time
   (format "MemoryEventStore store-identity, id: %s" (:id identity))
   (let [service-name (:service-name ctx)
         id-fn (juxt :identity :service-name)
         identity-with-service (assoc identity :service-name service-name)
         id (id-fn identity-with-service)
         store (get-realm-store ctx :identity-store)
         id-already-exists (some #(= (id-fn %) id) store)]
     (when id-already-exists
       (throw (RuntimeException. "Identity already exists")))
     (update-realm-store! ctx :identity-store
                          (fn [v] (conj (or v []) identity-with-service))))))

(defn deterministic-shuffle
  [^java.util.Collection coll seed]
  (let [al (java.util.ArrayList. coll)
        rng (java.util.Random. seed)]
    (java.util.Collections/shuffle al rng)
    (clojure.lang.RT/vector (.toArray al))))

(defn enqueue [q item seed]
  (vec (deterministic-shuffle (conj (or q []) item) seed)))

(defn peek-cmd!
  []
  (let [popq (fn [q] (if (seq q) (pop q) []))
        [old new] (swap-vals! (:command-queue *queues*) popq)]
    (peek old)))

(defn enqueue-cmd! [cmd]
  (swap! (:command-queue *queues*) enqueue cmd (:seed *queues*)))

(defn clean-commands
  [cmd]
  cmd)

(defn store-command
  [ctx cmd]
  (util/d-time
   (format "MemoryEventStore store-cmd, request-id: %s" (:request-id cmd))
   (update-realm-store! ctx :command-store
                        (fn [v] (conj (or v []) (clean-commands cmd))))
   (enqueue-cmd! cmd)))

(defn get-stored-commands
  []
  (get *dal-state* :command-store []))

(defn store-event
  [ctx event]
  (util/d-time
   (format "MemoryEventStore store-event, id: %s, event-seq: %s" (:id event) (:event-seq event))
   (let [aggregate-id (:id event)
         store (get-realm-store ctx :event-store)]
     (when (some (fn [e]
                   (and (= (:id e)
                           aggregate-id)
                        (= (:event-seq e)
                           (:event-seq event))))
                 store)
       (throw (ex-info "Already existing event" {:id        aggregate-id
                                                 :event-seq (:event-seq event)})))
     (update-realm-store! ctx :event-store
                          (fn [v] (conj (or v []) event))))))

(defn store-events
  [events])

(defn store-results-impl
  [{:keys [resp] :as ctx}]
  (let [resp (fix-keys resp)]
    (log-response ctx)
    (doseq [i (:events resp)]
      (store-event ctx i))
    (doseq [i (:identities resp)]
      (store-identity ctx i))
    (doseq [i (:effects resp)]
      (store-command ctx i))
    (log/info resp)
    (log/info "Emulated 'with-transaction' dal function")
    ctx))

(defmethod store-results
  :memory
  [ctx]
  (store-results-impl ctx))

(defmethod get-events
  :memory
  [{:keys [id version] :as ctx}]
  {:pre [id]}
  "Reads event from vector under :event-store key"
  (util/d-time
   (format "MemoryEventStore get-events, id: %s, version: %s" id version)
   (->> (get-realm-store ctx :event-store)
        (filter #(and (= (:id %) id)
                      (if version (> (long (:event-seq %)) (long version)) true)))
        (into [])
        (sort-by #(:event-seq %)))))

(defmethod get-command-response
  :memory
  [{:keys [request-id breadcrumbs] :as ctx}]

  (util/d-time
   (format "MemoryEventStore get-command-response-log, request-id: %s" request-id)
   (when (and request-id breadcrumbs)
     (let [store (get-realm-store ctx :response-log)]
       (first
        (filter #(and (= (:request-id %) request-id)
                      (= (:breadcrumbs %) breadcrumbs))
                store))))))

(defn get-max-event-seq-impl
  [{:keys [id] :as ctx}]
  (log/info "Emulated 'get-max-event-seq' dal function with fixed return value 0")
  (let [resp (map
              #(:event-seq %)
              (filter
               #(= (:id %) id)
               (get-realm-store ctx :event-store)))]
    (if (> (count resp) 0)
      (apply
       max
       resp)
      0)))

(defmethod get-max-event-seq
  :memory
  [ctx]
  (get-max-event-seq-impl ctx))

(defmethod get-aggregate-id-by-identity
  :memory
  [{:keys [identity service-name] :as ctx}]
  {:pre [identity]}
  (util/d-time
   (format "MemoryEventStore get-aggregate-id-by-identity, identity: %s" identity)
   (let [store (get-realm-store ctx :identity-store)]
     (if (coll? identity)
       (->> store
            (filter (fn [it]
                      (and (= (:service-name it) service-name)
                           (some #(= % (:identity it))
                                 identity))))
            (reduce
             (fn [p v]
               (assoc p
                      (:identity v)
                      (:id v)))
             {}))

       (->> store
            (filter #(and (= (:identity %) identity)
                          (= (:service-name %) service-name)))
            (first)
            :id)))))

(defn log-request-impl
  [ctx body]
  (util/d-time
   (format "MemoryEventStore log-request, request-id: %s" (:request-id body))
   (update-realm-store! ctx :command-log
                        (fn [v] (conj (or v []) body)))))

(defmethod log-request
  :memory
  [ctx body]
  (log-request-impl ctx body))

(defmethod log-request-error
  :memory
  [{:keys [request-id] :as ctx} body error]
  (util/d-time
   (format "MemoryEventStore log-request-error, request-id: %s" request-id)
   (update-realm-store! ctx :request-error-log
                        (fn [v] (conj (or v []) {:request-id  request-id
                                                 :breadcrumbs (:breadcrumbs body)
                                                 :error       error})))))

(defmethod log-response
  :memory
  [{:keys [response-summary request-id breadcrumbs] :as ctx}]
  (util/d-time
   (format "MemoryEventStore log-response, request-id: %s" request-id)
   (update-realm-store! ctx :response-log
                        (fn [v] (conj (or v []) {:request-id  request-id
                                                 :breadcrumbs breadcrumbs
                                                 :data        response-summary})))))

(defmethod with-init
  :memory
  [ctx body-fn]
  (log/debug "Initializing memory event store")
  (if-not (:global @*dal-state*)
    (body-fn ctx)
    (binding [*dal-state* (atom {})]
      (body-fn ctx))))

(defn register
  [ctx]
  (assoc ctx :edd-event-store :memory))

(defmethod get-records
  :memory
  [ctx {:keys [interaction-id]}]
  {:pre [interaction-id]}
  (let [events (filter
                #(= (:interaction-id %) interaction-id)
                (get-realm-store ctx :event-store))
        effects (filter
                 #(= (:interaction-id %) interaction-id)
                 (get-realm-store ctx :command-store))]
    {:events events
     :effects effects}))
