(ns pong.main
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [edd.java-lambda-runtime.core :as runtime]
            [edd.dynamodb.event-store :as event-store]
            [edd.s3.view-store :as view-store]
            [edd.common :as common]
            [edd.core :as edd]
            [lambda.util :as util]
            [lambda.filters :as filters]))

(def max-hops 5)

;; No-auth API filter for the E2E only: unlike lambda.filters/from-api it skips
;; JWT/Cognito and takes realm + user straight from the request body.
(def from-api-public
  {:cond (fn [{:keys [body]}]
           (contains? body :path))
   :fn (fn [{:keys [req] :as ctx}]
         (let [request (util/to-edn (:body req))]
           (assoc ctx
                  :from-api true
                  :body request
                  :meta (:meta request)
                  :user {:id "e2e" :role :non-interactive})))})

(defn current-dep
  [_deps cmd]
  {:query-id :get-by-id
   :id (:id cmd)})

(defn cmd-pong
  [ctx cmd]
  (let [hops (long (get-in cmd [:attrs :hops] 0))
        previous (get-in ctx [:current :hops] -1)]
    (log/info "PONG received, hops:" hops "previous:" previous)
    {:event-id :ponged
     :id (:id cmd)
     :attrs {:hops hops}}))

(defn evt-ponged
  [agg event]
  (-> agg
      (assoc :last :ponged)
      (assoc :hops (get-in event [:attrs :hops]))
      (update :pong-count (fnil inc 0))))

(defn cmd-set-value
  [_ctx cmd]
  {:event-id :value-set
   :id (:id cmd)
   :attrs {:value (get-in cmd [:attrs :value])}})

(defn evt-value-set
  [agg event]
  (assoc agg :value (get-in event [:attrs :value])))

;; Remote (cross-service) dependency: synchronously queries ping-svc over the API.
(def ping-value-dep
  {:service :ping-svc
   :query (fn [_deps cmd]
            {:query-id :get-by-id
             :id (get-in cmd [:attrs :ping-id])})})

(defn cmd-combine
  [ctx cmd]
  (let [ping-agg (:ping ctx)]
    {:event-id :combined
     :id (:id cmd)
     :attrs {:ping-value (:value ping-agg)
             :ping-version (:version ping-agg)
             :pong-value (get-in cmd [:attrs :value])}}))

(defn evt-combined
  [agg event]
  (-> agg
      (assoc :ping-value (get-in event [:attrs :ping-value]))
      (assoc :ping-version (get-in event [:attrs :ping-version]))
      (assoc :pong-value (get-in event [:attrs :pong-value]))))

(defn fx-ponged
  [_ctx event]
  (let [hops (long (get-in event [:attrs :hops] 0))]
    (when (< hops max-hops)
      {:service :ping-svc
       :commands [{:cmd-id :ping
                   :id (:id event)
                   :attrs {:hops (inc hops)}}]})))

(defn query-summary
  [ctx _query]
  (let [agg (:agg ctx)]
    {:id (:id agg)
     :hops (:hops agg)
     :pong-count (:pong-count agg)
     :last (:last agg)}))

(defn summary-dep
  [_deps query]
  {:query-id :get-by-id
   :id (:id query)})

(defn register [ctx]
  (-> ctx
      (assoc :service-name :pong-svc)
      (edd/reg-cmd :pong cmd-pong
                   :deps [:current current-dep])
      (edd/reg-cmd :combine cmd-combine
                   :deps [:ping ping-value-dep])
      (edd/reg-cmd :set-value cmd-set-value)
      (edd/reg-event :ponged evt-ponged)
      (edd/reg-event :combined evt-combined)
      (edd/reg-event :value-set evt-value-set)
      (edd/reg-event-fx :ponged fx-ponged)
      (edd/reg-query :get-by-id common/get-by-id)
      (edd/reg-query :summary query-summary
                     :deps [:agg summary-dep])))

(runtime/start
 (register (-> {}
               (event-store/register)
               (view-store/register)))
 edd/handler
 :filters [from-api-public
           filters/from-queue
           filters/from-bucket]
 :post-filter filters/to-api)
