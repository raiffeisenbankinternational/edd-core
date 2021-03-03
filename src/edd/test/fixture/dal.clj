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
            [clojure.data :refer [diff]]
            [clojure.test :refer :all]
            [edd.el.cmd :as cmd]
            [edd.core :as edd]
            [lambda.util :as util]
            [edd.common :as common]
            [lambda.uuid :as uuid]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.client :as client]
            [lambda.test.fixture.state :refer [*dal-state* *queues*]]
            [lambda.request :as request]))


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
    (:search condition) (full-search-cond (:search condition)))
  :default (fn [x] false))






;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   Test Fixtures   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;




(def default-db
  {:event-store     []
   :identity-store  []
   :sequence-store  []
   :command-store   []
   :aggregate-store []})

(defn create-identity
  [& [id]]
  (get-in @*dal-state* [:identities id] (uuid/gen)))

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
        (-> {:post (cmd/calc-service-url (:service %))
             :body (util/to-json {:result (:resp %)})
             :req  req-2})))

    (get @*dal-state* :dps [])))


(defn aws-get-token
  [ctx]
  "#mock-id-token")

(def ctx
  (-> {}
      (view-store/register)
      (event-store/register)))

(defmacro with-mock-dal [& body]
  `(edd/with-stores
     ctx
     #(binding [*dal-state* (atom ~(if (map? (first body))
                                     (merge
                                      default-db
                                      (first body))
                                     default-db))
                *queues*     (atom {:command-queue []})
                util/*cache* (atom {})
                request/*request* (atom {})]
        %
        (client/mock-http
         (prepare-dps-calls)
         (with-redefs
           [aws/get-token aws-get-token
            common/create-identity create-identity]
           (do ~@body))))))

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

(defn handle-cmd [ctx cmd]
  (if (contains? cmd :commands)
    (cmd/handle-commands ctx cmd)
    (cmd/handle-commands ctx {:commands [cmd]})))

(defn apply-cmd [ctx cmd]
  (log/info "apply-cmd" cmd)
  (let [resp (handle-cmd (assoc ctx
                           :no-summary true) cmd)]
    (log/info "apply-cmd returned" resp)
    (doseq [id (distinct (map :id (:events resp)))]
      (event/handle-event (assoc ctx
                            :apply {:aggregate-id id})))))


(defn execute-fx [ctx]
  (doall
    (for [cmd (pop-state :command-store)]
      (cmd/handle-commands ctx cmd))))

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
