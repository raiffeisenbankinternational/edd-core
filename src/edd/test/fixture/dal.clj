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
  {:event-store     []
   :identity-store  []
   :sequence-store  []
   :command-store   []
   :aggregate-store []})

(defn create-identity
  [& [id]]
  (get-in @*dal-state* [:identities (keyword id)] (uuid/gen)))

(defn prepare-dps-calls
  []
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
       (-> {:post (cmd/calc-service-query-url (:service %))
            :body (util/to-json {:result (:resp %)})
            :req  req-2})))

   (get @*dal-state* :dps [])))

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

(defmacro with-mock-dal [& body]
  `(edd/with-stores
     ctx
     #(binding [*dal-state* (atom (util/fix-keys
                                   ~(if (map? (first body))
                                      (merge
                                       default-db
                                       (dissoc (first body) :seed))
                                      default-db)))
                *queues* {:command-queue (atom [])
                          :seed          ~(if (and (map? (first body)) (:seed (first body)))
                                            (:seed (first body))
                                            '(rand-int 10000000))}
                util/*cache* (atom {})
                request/*request* (atom {})]
        %
        (client/mock-http
         (vec (concat (prepare-dps-calls)
                      (get @*dal-state* :responses [])))
         (with-redefs
          [aws/get-token aws-get-token
           common/create-identity create-identity]
           (do (log/info "with-mock-dal using seed" (:seed *queues*))
               ~@body))))))

(defmacro verify-state [x & [y]]
  `(if (keyword? ~y)
     (is (= ~x (into [] (~y @*dal-state*))))
     (is (= ~y (into [] (~x @*dal-state*))))))

(defmacro verify-state-fn [x fn y]
  `(is (= ~y (mapv
              ~fn
              (~x @*dal-state*)))))

(defn pop-state
  "Retrieves commands and removes them from the store"
  [x]
  (let [current-state (x @*dal-state*)]
    (swap! *dal-state*
           #(update % x (fn [v] [])))
    current-state))

(defn peek-state
  "Retrieves the first command without removing it from the store"
  [& x]
  (if x
    ((first x) @*dal-state*)
    @*dal-state*))

(defn- re-parse
  "Sometimes we parse things from outside differently
  because keywordize keys is by default. If you have map of string
  keys they would be keywordized. But if we pass in to test map
  that has string we would receive string. So here we re-parse requests"
  [cmd]
  (util/fix-keys cmd))

(defn handle-cmd
  [{:keys [include-meta no-summary] :as ctx} cmd]
  (try
    (let [resp (if (contains? cmd :commands)
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
