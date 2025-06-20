(ns edd.memory.event-store
  (:require
   [clojure.tools.logging :as log]
   [lambda.test.fixture.state :refer [*dal-state* *queues*]]
   [edd.dal :refer [with-init
                    get-events
                    get-max-event-seq
                    get-sequence-number-for-id
                    get-id-for-sequence-number
                    get-aggregate-id-by-identity
                    get-command-response
                    log-dps
                    log-request
                    log-request-error
                    log-response
                    store-results]]
   [lambda.util :as util]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn fix-keys
  [val]
  (-> val
      (util/to-json)
      (util/to-edn)))

(defn store-sequence
  "Stores sequence in memory structure.
  Raises RuntimeException if sequence is already taken"
  [{:keys [id]}]
  {:pre [id]}
  (log/info "Emulated 'store-sequence' dal function")
  (let [store (:sequence-store @*dal-state*)
        sequence-already-exists (some
                                 #(= (:id %) id)
                                 store)
        max-number (count store)]
    (if sequence-already-exists
      (throw (RuntimeException. "Sequence already exists")))
    (swap! *dal-state*
           #(update % :sequence-store (fn [v] (conj v {:id    id
                                                       :value (inc max-number)}))))))

(defn store-identity
  "Stores identity in memory structure.
  Raises RuntimeException if identity is already taken"
  [identity]
  (log/info "Emulated 'store-identity' dal function")
  (let [id-fn (juxt :identity)
        id (id-fn identity)
        store (:identity-store @*dal-state*)
        id-already-exists (some #(= (id-fn %) id) store)]
    (when id-already-exists
      (throw (RuntimeException. "Identity already exists")))
    (swap! *dal-state*
           #(update % :identity-store (fn [v] (conj v (dissoc identity
                                                              :request-id
                                                              :interaction-id
                                                              :meta)))))))

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
  (dissoc cmd
          :request-id
          :interaction-id
          :breadcrumbs))

(defn store-command
  "Stores command in memory structure"
  [cmd]
  (log/info "Emulated 'store-cmd' dal function")
  (swap! *dal-state*
         #(update % :command-store (fnil conj []) (clean-commands cmd)))
  (enqueue-cmd! cmd))

(defn get-stored-commands
  []
  (get *dal-state* :command-store []))

(defn store-event
  "Stores event in memory structure"
  [event]
  (log/info "Emulated 'store-event' dal function")
  (let [aggregate-id (:id event)]
    (when (some (fn [e]
                  (and (= (:id e)
                          aggregate-id)
                       (= (:event-seq e)
                          (:event-seq event))))
                (:event-store @*dal-state*))
      (throw (ex-info "Already existing event" {:id        aggregate-id
                                                :event-seq (:event-seq event)})))
    (swap! *dal-state*
           #(update % :event-store (fn [v] (sort-by
                                            (fn [c] (:event-seq c))
                                            (conj v (dissoc
                                                     event
                                                     :interaction-id
                                                     :request-id))))))))
(defn store-events
  [events])

(defn store-results-impl
  [{:keys [resp] :as ctx}]
  (let [resp (fix-keys resp)]
    (log-response ctx)
    (doseq [i (:events resp)]
      (store-event i))
    (doseq [i (:identities resp)]
      (store-identity i))
    (doseq [i (:sequences resp)]
      (store-sequence i))
    (doseq [i (:effects resp)]
      (store-command i))
    (log/info resp)
    (log/info "Emulated 'with-transaction' dal function")
    ctx))

(defmethod store-results
  :memory
  [ctx]
  (store-results-impl ctx))

(defmethod get-events
  :memory
  [{:keys [id version]}]
  {:pre [id]}
  "Reads event from vector under :event-store key"
  (log/info "Emulated 'get-events' dal function")
  (->> @*dal-state*
       (:event-store)
       (filter #(and (= (:id %) id)
                     (if version (> (long (:event-seq %)) (long version)) true)))
       (into [])
       (sort-by #(:event-seq %))))

(defmethod get-sequence-number-for-id
  :memory
  [{:keys [id]}]
  {:pre [id]}
  (let [store (:sequence-store @*dal-state*)
        entry (first (filter #(= (:id %) id)
                             store))]
    (:value entry)))

(defmethod get-command-response
  :memory
  [{:keys [request-id breadcrumbs]}]

  (log/info "Emulating get-command-response-log" request-id breadcrumbs)
  (when (and request-id breadcrumbs)
    (let [store (:response-log @*dal-state*)]
      (first
       (filter #(and (= (:request-id %) request-id)
                     (= (:breadcrumbs %) breadcrumbs))
               store)))))

(defmethod get-id-for-sequence-number
  :memory
  [{:keys [sequence]}]
  {:pre [sequence]}
  (let [store (:sequence-store @*dal-state*)
        entry (first (filter #(= (:value %) sequence)
                             store))]
    (:id entry)))

(defn get-max-event-seq-impl
  [{:keys [id]}]
  (log/info "Emulated 'get-max-event-seq' dal function with fixed return value 0")
  (let [resp (map
              #(:event-seq %)
              (filter
               #(= (:id %) id)
               (:event-store @*dal-state*)))]
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
  [{:keys [identity]}]
  {:pre [identity]}
  (log/info "Emulating get-aggregate-id-by-identity" identity)
  (let [store (:identity-store @*dal-state*)]
    (if (coll? identity)
      (->> store
           (filter (fn [it]
                     (some #(= %
                               (:identity it))
                           identity)))
           (reduce
            (fn [p v]
              (assoc p
                     (:identity v)
                     (:id v)))
            {}))

      (->> store
           (filter #(= (:identity %) identity))
           (first)
           :id))))

(defn log-request-impl
  [_ctx body]
  (log/info "Storing mock request" body)
  (swap! *dal-state*
         #(update % :command-log (fn [v] (conj v body)))))

(defmethod log-request
  :memory
  [ctx body]
  (log-request-impl ctx body))

(defmethod log-request-error
  :memory
  [ct body error]
  (log/info "Should store mock request error" body error))

(defmethod log-dps
  :memory
  [{:keys [dps-resolved] :as ctx}]
  (log/debug "Storing mock dps" dps-resolved)
  (swap! *dal-state*
         #(update % :dps-log (fn [v] (conj v dps-resolved))))
  ctx)

(defmethod log-response
  :memory
  [{:keys [response-summary request-id breadcrumbs]}]
  (log/info "Storing mock response" response-summary)
  (swap! *dal-state*
         #(update % :response-log (fn [v] (conj v {:request-id  request-id
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
  (assoc ctx :event-store :memory))
