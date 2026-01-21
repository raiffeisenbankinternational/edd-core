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
  (:require [clojure.tools.logging :as log]
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
            [aws.aws :as aws]))

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

(defn dal-state-accessor
  "Returns realm-scoped store data with normalization for test assertions.
   Removes request-id, interaction-id, service-name.
   Sorts event-store by event-seq."
  [state key]
  (if (#{:event-store :identity-store :command-store :aggregate-store
         :response-log :command-log :request-error-log} key)
    (let [realm (get state :realm :test)
          data (get-in state [:realms realm key] [])
          normalize (fn [item] (dissoc item :request-id :interaction-id :service-name))]
      (if (= key :event-store)
        (mapv normalize (sort-by :event-seq data))
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
                   (:meta ctx))
            :body (util/to-json {:result (:resp %)})
            :req  req-2})))

   (get @*dal-state* :dps
        (get @*dal-state* :deps []))))

(defn aws-get-token
  [ctx]
  "#mock-id-token")

(def ctx
  (-> {:response-schema-validation :throw-on-error}
      (response-cache/register-default)
      (view-store/register)
      (event-store/register)))

(defn mock-snapshot
  [_ _]
  nil)

(defn create-realm-partitioned-state
  "Partitions flat test state into realm structure.
   Moves top-level store keys into [:realms <realm> <store-key>]."
  [base-state]
  (let [realm (get base-state :realm :test)
        store-keys [:event-store :identity-store :command-store :aggregate-store
                    :response-log :command-log :request-error-log]
        top-level-stores (select-keys base-state store-keys)
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
        (binding [*dal-state* (atom (create-realm-partitioned-state base-state#))
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
  `(if (keyword? ~y)
     (is (= ~x (into [] (dal-state-accessor @*dal-state* ~y))))
     (is (= ~y (into [] (dal-state-accessor @*dal-state* ~x))))))

(defmacro verify-state-fn [x fn y]
  `(is (= ~y (mapv
              ~fn
              (dal-state-accessor @*dal-state* ~x)))))

(defn pop-state
  "Retrieves and clears realm-scoped store."
  [store-key]
  (let [realm (get-realm-from-state)
        current-state (get-in @*dal-state* [:realms realm store-key] [])]
    (swap! *dal-state*
           #(assoc-in % [:realms realm store-key] []))
    current-state))

(defn peek-state
  "Retrieves normalized realm-scoped store data without removing it.
   
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
        (do
          (if no-summary
            (do
              (-> resp
                  (update :events #(map
                                    (fn [event]
                                      (dissoc event
                                              :request-id
                                              :interaction-id
                                              :meta))
                                    %))
                  (update :effects #(map
                                     (fn [cmd]
                                       (dissoc cmd
                                               :request-id
                                               :interaction-id
                                               :meta))
                                     %))))
            resp))))
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
    (log/info "apply-cmd returned" resp)
    (doseq [id (distinct (map :id (:events resp)))]
      (event/handle-event (assoc ctx
                                 :apply {:aggregate-id id
                                         :meta         (:meta ctx {})})))))

(defn execute-fx [ctx]
  (doall
   (for [cmd (pop-state :command-store)]
     (handle-cmd ctx cmd))))

(defn execute-fx-apply [ctx]
  (doall
   (for [{:keys [commands]} (pop-state :command-store)]
     (doall
      (for [cmd commands]
        (apply-cmd ctx cmd))))))

(defn execute-fx-apply-all
  "Executes all the side effects until the command store is empty"
  [ctx]
  (while (seq (peek-state :command-store))
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
