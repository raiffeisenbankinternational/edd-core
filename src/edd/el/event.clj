(ns edd.el.event
  (:require
   [edd.flow :refer :all]
   [clojure.tools.logging :as log]
   [edd.dal :as dal]
   [lambda.request :as request]
   [edd.search :as search]))

(defn apply-event
  [agr event func]
  (if func
    (assoc
     (apply func [agr event])
     :version (:event-seq event)
     :id (:id event))
    agr))

(defn create-aggregate
  [snapshot events apply-functions]
  (reduce
   (fn [agr event]
     (log/debug "Attempting to apply" event)
     (let [event-id (keyword (:event-id event))]

       (if (contains? apply-functions event-id)
         (apply-event
          agr
          event
          (event-id apply-functions))
         agr)))
   snapshot
   events))

(defn apply-agg-filter
  [ctx aggregate]
  (reduce
   (fn [v f]
     (f (assoc
         ctx
         :agg v)))
   aggregate
   (get ctx :agg-filter [])))

(defn get-current-state
  [{:keys [id events snapshot] :as ctx}]
  {:pre [id events]}
  (log/debug "Updating aggregates" id)
  (log/debug "Events: " events)
  (log/debug "Snapshot: " snapshot)

  (cond
    (:error events) (throw (ex-info "Error fetching events" {:error events}))
    (> (count events) 0) (let [aggregate (create-aggregate snapshot events (:def-apply ctx))
                               result-agg (apply-agg-filter ctx aggregate)]
                           (assoc
                            ctx
                            :aggregate result-agg))
    snapshot (assoc ctx :aggregate snapshot)
    :else (throw (ex-info "Aggregate not found" {:error :no-events-found}))))

(defn fetch-snapshot
  [ctx]
  (if-let [snapshot
           (first
            (search/simple-search
             (assoc ctx
                    :query {:id (:id ctx)})))]

    (assoc ctx
           :snapshot snapshot
           :version (:version snapshot))
    ctx))

(defn get-events [ctx]
  (assoc ctx :events (dal/get-events ctx)))

(defn get-by-id
  [ctx]
  {:pre [(:id ctx)]}
  (-> ctx
      (fetch-snapshot)
      (get-events)
      (get-current-state)))

(defn update-aggregate
  [ctx]
  (if (:aggregate ctx)
    (search/update-aggregate ctx)
    ctx))

(defn handle-event
  [{:keys [apply] :as ctx}]
  (try
    (let [agg-id (:aggregate-id apply)]
      (if (:scoped @request/*request*)
        (let [applied (get-in @request/*request* [:applied agg-id])]
          (if-not applied
            (do (-> ctx
                    (assoc :id agg-id)
                    (get-by-id)
                    (update-aggregate))

                (swap! request/*request*
                       #(assoc-in % [:applied agg-id] {:apply true})))))
        (-> ctx
            (assoc :id agg-id)
            (get-by-id)
            (update-aggregate))))
    {:apply true}
    (catch Exception e
      (log/error e)
      (ex-data e))))
