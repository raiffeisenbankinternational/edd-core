(ns edd.el.event
  (:require
   [clojure.tools.logging :as log]
   [edd.dal :as dal]))

(defn apply-event
  [agr event func]
  (if func
    (assoc
     (apply func [agr event])
     :id
     (:id event))))

(defn get-current-state
  [ctx agg-id]
  (log/debug "Updating aggregates" agg-id)
  (let [events (:events ctx)]
    (log/debug "Events: " events)

    (if (> (count events) 0)
      (let [result-aggregate (reduce
                              (fn [agr event]
                                (log/debug "Attempting to apply" event)
                                (let [apply-functions (:apply ctx)
                                      event-id (keyword (:event-id event))]

                                  (if (contains? apply-functions event-id)
                                    (apply-event
                                     agr
                                     event
                                     (event-id apply-functions))
                                    agr)))
                              nil
                              events)]
        (reduce
         (fn [v f]
           (f (assoc
               ctx
               :agg v)))
         result-aggregate
         (get ctx :agg-filter [])))
      nil)))

(defn fetch-events-to-ctx
  [ctx agg-id]
  (dal/get-events ctx agg-id))

(defn handle-event
  [ctx body]
  (let [agg-id (:aggregate-id (:apply body))
        events (fetch-events-to-ctx ctx agg-id)]
    (log/info "Updating aggregate " agg-id)
    (log/info "Events" events)
    (if (:error events)
      events
      (let [current-state (get-current-state
                           (assoc ctx
                                  :events
                                  events)
                           agg-id)]
        (if current-state
          (dal/update-aggregate ctx current-state)
          {:apply true})))))



