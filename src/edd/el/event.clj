(ns edd.el.event
  (:require
   [edd.flow :refer :all]
   [clojure.tools.logging :as log]
   [edd.dal :as dal]
   [edd.search :as search]))

(defn apply-event
  [agr event func]
  (if func
    (assoc
      (apply func [agr event])
      :version (:event-seq event)
      :id (:id event))))

(defn get-current-state
  [{:keys [id events snapshot] :as ctx}]
  {:pre [id events]}
  (log/debug "Updating aggregates" id)
  (log/debug "Events: " events)
  (log/debug "Snapshot: " snapshot)

  (cond
    (:error events) (assoc ctx :error events)
    (> (count events) 0) (let [result-aggregate (reduce
                                                 (fn [agr event]
                                                   (log/debug "Attempting to apply" event)
                                                   (let [apply-functions (:def-apply ctx)
                                                         event-id (keyword (:event-id event))]

                                                     (if (contains? apply-functions event-id)
                                                       (apply-event
                                                        agr
                                                        event
                                                        (event-id apply-functions))
                                                       agr)))
                                                 snapshot
                                                 events)]
                           (log/info "RE" result-aggregate)
                           (assoc
                             ctx
                             :aggregate (reduce
                                         (fn [v f]
                                           (f (assoc
                                                ctx
                                                :agg v)))
                                         result-aggregate
                                         (get ctx :agg-filter []))))
    snapshot (assoc ctx :aggregate snapshot)
    :else (assoc ctx :error :no-events-found)))

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


(defn summarize-result
  [{:keys [aggregate]}]
  (if aggregate
    {:apply true}
    {:error :no-aggregate-found}))

(defn handle-event
  [{:keys [apply] :as ctx}]
  (e-> ctx
       (assoc :id (:aggregate-id apply))
       (get-by-id)
       (search/update-aggregate)
       (summarize-result)))
