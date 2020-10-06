;; This namespace functions emulating Data Access Layer
;; by redefining Data Access Layer function and binding
;; state to thread local. Should be used only in tests.
;; Any test wanted to use emulated Data Access Layer
;; should call "defdaltest" macro.
;;
;; Example:
;;
;; (defdaltest when-store-and-load-events
;;   (dal/store-event {} {} {:id 1 :info "info"})
;;   (verify-state [{:id 1 :info "info"}] :event-store)
;;   (let [events (dal/get-events {} 1)]
;;     (is (= [{:id 1 :info "info"}]
;;            events))))
;;

(ns edd.test.fixture.dal
  (:require [clojure.tools.logging :as log]
            [edd.dal :as dal]
            [edd.common :as common]
            [edd.el.event :as event]
            [clojure.data :refer [diff]]
            [clojure.test :refer :all]
            [edd.el.cmd :as cmd]
            [edd.search :as search]
            [edd.test.fixture.search :as search-mock]
            [lambda.util :as util]
            [lambda.test.fixture.client :as client]
            [lambda.test.fixture.state :refer [*dal-state*]]))



;; The structure of this atom represent database
;;
;; :event-store - event store table
;; :identity-store - identity store table
;; :sequence-store - sequence store table
;; :command-store - command store table
;; :aggregate-store - aggregate store table




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Data Access Layer ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn update-aggregate
  "Creates or Overrides the aggregate in memory structure."
  [_ agr]
  (log/info "Emulated 'update-aggregate' dal function")
  (swap! *dal-state*
         #(update % :aggregate-store
                  (fn [v]
                    (conj (filter
                           (fn [el]
                             (not= (:id el) (:id agr)))
                           v)
                          agr)))))

(defn store-sequence
  "Stores sequence in memory structure.
  Raises RuntimeException if sequence is already taken"
  [ctx sequence]
  {:pre [(:id sequence)]}
  (log/info "Emulated 'store-sequence' dal function")
  (let [aggregate-id (:id sequence)
        store (:sequence-store @*dal-state*)
        sequence-already-exists (some
                                 #(= (:id %) aggregate-id)
                                 store)
        max-number (count store)]
    (if sequence-already-exists
      (throw (RuntimeException. "Sequence already exists")))
    (swap! *dal-state*
           #(update % :sequence-store (fn [v] (conj v {:id    aggregate-id
                                                       :value (inc max-number)}))))))

(defn query-sequence-number-for-id [ctx query]
  {:pre [(:id query)]}
  (let [store (:sequence-store @*dal-state*)
        entry (first (filter #(= (:id %) (:id query))
                             store))]
    (:value entry)))

(defn query-id-for-sequence-number [ctx query]
  {:pre [(:value query)]}
  (let [store (:sequence-store @*dal-state*)
        entry (first (filter #(= (:value %) (:value query))
                             store))]
    (:id entry)))

(defn store-identity
  "Stores identity in memory structure.
  Raises RuntimeException if identity is already taken"
  [_ identity]
  (log/info "Emulated 'store-identity' dal function")
  (let [id (:identity identity)
        store (:identity-store @*dal-state*)
        identity-already-exists (some #(= (:identity %) id) store)]
    (if identity-already-exists
      (throw (RuntimeException. "Identity already exists")))
    (swap! *dal-state*
           #(update % :identity-store (fn [v] (conj v identity))))))

(defn store-cmd
  "Stores command in memory structure"
  [_ cmd]
  (log/info "Emulated 'store-cmd' dal function")
  (swap! *dal-state*
         #(update % :command-store (fn [v] (conj v (dissoc
                                                    cmd
                                                    :request-id
                                                    :interaction-id))))))

(defn read-realm
  "Currently does nothing"
  [_]
  (log/info "Emulated 'read-realm' dal function"))

(defn with-transaction
  "We have build in STM for atom no external transaction is needed"
  [ctx func]
  (log/info "Emulated 'with-transaction' dal function")
  (func ctx))

(defn store-event
  "Stores event in memory structure"
  [_ _ event]
  (log/info "Emulated 'store-event' dal function")
  (let [aggregate-id (:id event)]
    (swap! *dal-state*
           #(update % :event-store (fn [v] (sort-by
                                            (fn [c] (:event-seq c))
                                            (conj v (dissoc
                                                     event
                                                     :interaction-id
                                                     :request-id))))))))

(defn get-events
  "Reads event from vector under :event-store key"
  [_ id]
  (log/info "Emulated 'get-events' dal function")
  (->> @*dal-state*
       (:event-store)
       (filter #(= (:id %) id))
       (into [])
       (sort-by #(:event-seq %))))

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
    (:search condition) (full-search-cond (:search condition)))
  :default (fn [x] false))

(defn filter-aggregate
  [query aggregate]
  (let [res (diff aggregate query)]
    (and (= (second res) nil)
         (= (nth res 2) query))))

(defn simple-search
  [_ q]
  (into []
        (filter
         #(filter-aggregate
           (dissoc q :query-id)
           %)
         (->> @*dal-state*
              (:aggregate-store)))))

(defn get-max-event-seq
  "Emulate get-max-event-seq"
  [_ id]
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

(defn get-by-id
  [ctx id]
  (log/info "Emulating get-by-id" id)
  (let [store (:event-store @*dal-state*)]
    (let [result (into []
                       (filter #(= (:id %) (:id id)) store))]
      (if (> (count result) 0)
        (event/get-current-state
         (assoc ctx :events result)
         (:id id))))))

(defn get-aggregate-id-by-identity
  [_ identity]
  (log/info "Emulating get-aggregate-id-by-identity" identity)
  (let [store (:identity-store @*dal-state*)]
    (:id
     (first
      (filter #(= (:identity %) identity) store)))))

(defn log-request
  [{:keys [commands]}]
  (log/debug "Storing mock request" commands)
  (swap! *dal-state*
         #(update % :command-log (fn [v] (conj v commands)))))

(defn log-dps
  [{:keys [dps-resolved] :as ctx}]
  (log/debug "Storing mock dps" dps-resolved)
  (swap! *dal-state*
         #(update % :dps-log (fn [v] (conj v dps-resolved))))
  ctx)

(defn log-response
  [{:keys [resp]}]
  (log/debug "Storing mock response" resp)
  (swap! *dal-state*
         #(update % :response-log (fn [v] (conj v resp)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   Test Fixtures   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn add-metadata
  [ctx ready-body transaction-response]
  transaction-response)

(def default-db
  {:event-store     []
   :identity-store  []
   :sequence-store  []
   :command-store   []
   :aggregate-store []})

(defn prepare-dps-calls
  []
  (mapv
   (fn [%]
     (let [req {:query
                (:query %)}
           req-1 (if (:request-id %)
                   (assoc req :request-id (:request-id %)))
           req-2 (if (:interaction-id %)
                   (assoc req-1 :interaction-id (:interaction-id %)))]
       (-> {:post (cmd/calc-service-url (:service %))
            :body (util/to-json {:result (:resp %)})
            :req  req-2})))

   (get @*dal-state* :dps [])))

(defn aws-get-token
  [ctx]
  "#mock-id-token")

(defmacro with-mock-dal [& body]
  `(binding [*dal-state* (atom ~(if (map? (first body))
                                  (merge
                                   default-db
                                   (first body))
                                  default-db))
             util/*cache* (atom {})]
     (client/mock-http
      (prepare-dps-calls)
      (with-redefs
       [aws/get-token aws-get-token
        dal/get-events get-events
        dal/read-realm read-realm
        dal/update-aggregate update-aggregate
        dal/store-sequence store-sequence
        dal/store-cmd store-cmd
        dal/store-identity store-identity
        dal/query-id-for-sequence-number query-id-for-sequence-number
        dal/query-sequence-number-for-id query-sequence-number-for-id
        dal/log-request log-request
        dal/log-dps log-dps
        dal/log-response log-response
        dal/with-transaction with-transaction
        dal/store-event store-event
        dal/get-max-event-seq get-max-event-seq
        dal/simple-search simple-search
        search/advanced-search search-mock/advanced-search
        cmd/add-metadata add-metadata
        common/get-by-id get-by-id
        common/get-aggregate-id-by-identity get-aggregate-id-by-identity]
        (do ~@body)))))

(defmacro verify-state [x y]
  `(if (keyword? ~y)
     (is (= ~x (into [] (~y @*dal-state*))))
     (is (= ~y (into [] (~x @*dal-state*))))))

(defn pop-state
  "Retrieves commands end removes them from store"
  [x]
  (let [current-state (x @*dal-state*)]
    (swap! *dal-state*
           #(update % x (fn [v] [])))
    current-state))

(defn peek-state
  "Retrieves commands end removes them from store"
  [& x]
  (if x
    ((first x) @*dal-state*)
    @*dal-state*))

(defn apply-cmd [ctx cmd]
  (cmd/get-commands-response ctx {:commands [cmd]})
  (event/handle-event ctx {:apply {:aggregate-id (:id cmd)}}))

(defn execute-fx [ctx]
  (doall
   (for [cmd (pop-state :command-store)]
     (cmd/get-commands-response ctx cmd))))

(defn execute-fx-apply [ctx]
  (doall
   (for [{:keys [commands]} (pop-state :command-store)]
     (doall
      (for [cmd commands]
        (apply-cmd ctx cmd))))))

