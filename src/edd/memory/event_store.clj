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
                    log-response
                    store-results]]))

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
  (let [value (:identity identity)
        id (:id identity)
        store (:identity-store @*dal-state*)
        identity-already-exists (some #(or (= (:identity %) value)
                                           (= (:id %) id)) store)]
    (if identity-already-exists
      (throw (RuntimeException. "Identity already exists")))
    (swap! *dal-state*
           #(update % :identity-store (fn [v] (conj v identity))))))

(defn enqueue [q item]
  (vec (shuffle (conj (or q []) item))))

(defn peek-cmd!
  []
  (let [popq (fn [q] (if (seq q) (pop q) []))
        [old new] (swap-vals! *queues* update :command-queue popq)]
    (peek (:command-queue old))))

(defn enqueue-cmd! [cmd]
  (swap! *queues*
         update :command-queue enqueue cmd))

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

(defn store-event
  "Stores event in memory structure"
  [event]
  (log/info "Emulated 'store-event' dal function")
  (let [aggregate-id (:id event)]
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
  (log-response ctx)
  (doseq [i (:events resp)]
    (store-event i))
  (doseq [i (:identities resp)]
    (store-identity i))
  (doseq [i (:sequences resp)]
    (store-sequence i))
  (doseq [i (:commands resp)]
    (store-command i))
  (log/info resp)
  (log/info "Emulated 'with-transaction' dal function")
  ctx)

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
                     (if version (> (:event-seq %) version) true)))
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
    (:id
     (first
      (filter #(= (:identity %) identity) store)))))

(defmethod log-request
  :memory
  [ct body]
  (log/info "Storing mock request" body)
  (swap! *dal-state*
         #(update % :command-log (fn [v] (conj v body)))))

(defmethod log-dps
  :memory
  [{:keys [dps-resolved] :as ctx}]
  (log/debug "Storing mock dps" dps-resolved)
  (swap! *dal-state*
         #(update % :dps-log (fn [v] (conj v dps-resolved))))
  ctx)

(defmethod log-response
  :memory
  [{:keys [resp request-id breadcrumbs]}]
  (log/info "Storing mock response" resp)
  (swap! *dal-state*
         #(update % :response-log (fn [v] (conj v {:request-id  request-id
                                                   :breadcrumbs breadcrumbs
                                                   :data        resp})))))

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
