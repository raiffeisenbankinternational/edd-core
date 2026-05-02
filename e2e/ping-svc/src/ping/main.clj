(ns ping.main
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

(defn cmd-ping
  [ctx cmd]
  (let [hops (long (get-in cmd [:attrs :hops] 0))
        previous (get-in ctx [:current :hops] -1)]
    (log/info "PING received, hops:" hops "previous:" previous)
    {:event-id :pinged
     :id (:id cmd)
     :attrs {:hops hops}}))

(defn cmd-object-uploaded
  [_ctx cmd]
  (log/info "PING object-uploaded" (:key cmd))
  {:event-id :object-recorded
   :id (:id cmd)
   :attrs {:bucket (:bucket cmd)
           :key (:key cmd)}})

(defn cmd-set-value
  [_ctx cmd]
  {:event-id :value-set
   :id (:id cmd)
   :attrs {:value (get-in cmd [:attrs :value])}})

(defn evt-value-set
  [agg event]
  (assoc agg :value (get-in event [:attrs :value])))

(defn cmd-claim-name
  [_ctx cmd]
  [{:identity (str "name/" (get-in cmd [:attrs :name]))}
   {:event-id :name-claimed
    :id (:id cmd)
    :attrs {:name (get-in cmd [:attrs :name])}}])

(defn evt-name-claimed
  [agg event]
  (assoc agg :name (get-in event [:attrs :name])))

(def SetScoreCmd
  [:map
   [:id uuid?]
   [:attrs [:map [:score pos-int?]]]])

(defn cmd-set-score
  [_ctx cmd]
  {:event-id :score-set
   :id (:id cmd)
   :attrs {:score (get-in cmd [:attrs :score])}})

(defn evt-score-set
  [agg event]
  (assoc agg :score (get-in event [:attrs :score])))

(defn cmd-broadcast
  [_ctx cmd]
  {:event-id :broadcasted
   :id (:id cmd)
   :attrs (:attrs cmd)})

(defn evt-broadcasted
  [agg _event]
  (assoc agg :last :broadcasted))

(defn fx-broadcasted
  [_ctx event]
  [{:service :ping-svc
    :commands [{:cmd-id :set-value
                :id (get-in event [:attrs :ping-target])
                :attrs {:value (get-in event [:attrs :value])}}]}
   {:service :pong-svc
    :commands [{:cmd-id :set-value
                :id (get-in event [:attrs :pong-target])
                :attrs {:value (get-in event [:attrs :value])}}]}])

(defn evt-pinged
  [agg event]
  (-> agg
      (assoc :last :pinged)
      (assoc :hops (get-in event [:attrs :hops]))
      (update :ping-count (fnil inc 0))))

(defn evt-object-recorded
  [agg event]
  (-> agg
      (assoc :last :object-recorded)
      (assoc :uploaded-key (get-in event [:attrs :key]))))

(defn fx-pinged
  [_ctx event]
  (let [hops (long (get-in event [:attrs :hops] 0))]
    (when (< hops max-hops)
      {:service :pong-svc
       :commands [{:cmd-id :pong
                   :id (:id event)
                   :attrs {:hops (inc hops)}}]})))

(defn query-summary
  [ctx _query]
  (let [agg (:agg ctx)]
    {:id (:id agg)
     :hops (:hops agg)
     :ping-count (:ping-count agg)
     :last (:last agg)}))

(defn summary-dep
  [_deps query]
  {:query-id :get-by-id
   :id (:id query)})

(defn register [ctx]
  (-> ctx
      (assoc :service-name :ping-svc)
      (edd/reg-cmd :ping cmd-ping
                   :deps [:current current-dep])
      (edd/reg-cmd :object-uploaded cmd-object-uploaded)
      (edd/reg-cmd :set-value cmd-set-value)
      (edd/reg-cmd :claim-name cmd-claim-name)
      (edd/reg-cmd :set-score cmd-set-score
                   :consumes SetScoreCmd)
      (edd/reg-cmd :broadcast cmd-broadcast)
      (edd/reg-event :pinged evt-pinged)
      (edd/reg-event :object-recorded evt-object-recorded)
      (edd/reg-event :value-set evt-value-set)
      (edd/reg-event :name-claimed evt-name-claimed)
      (edd/reg-event :score-set evt-score-set)
      (edd/reg-event :broadcasted evt-broadcasted)
      (edd/reg-event-fx :broadcasted fx-broadcasted)
      (edd/reg-event-fx :pinged fx-pinged)
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
