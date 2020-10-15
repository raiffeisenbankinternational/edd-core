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
      :id
      (:id event))))

(defn get-current-state
  [{:keys [id events] :as ctx}]
  {:pre [id events]}
  (log/debug "Updating aggregates" id)
  (log/debug "Events: " events)

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
                                                  nil
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

    :else (assoc ctx :error :no-events-found)))

(defn fetch-events
  [ctx]
  (let [events (-> ctx
                   (assoc :id (get-in ctx [:apply :aggregate-id]))
                   (dal/get-events))]
    (assoc ctx :events events)))




(defn summarize-result
  [{:keys [aggregate]}]
  (if aggregate
    {:apply true}
    {:error :no-aggregate-found}))

(defn handle-event
  [{:keys [apply] :as ctx}]
  (e-> ctx
       (fetch-events)
       (assoc :id (:aggregate-id apply))
       (get-current-state)
       (search/update-aggregate)
       (summarize-result)))



