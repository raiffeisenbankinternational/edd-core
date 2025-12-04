(ns edd.el.event
  (:require
   [edd.flow :refer :all]
   [clojure.tools.logging :as log]
   [edd.dal :as dal]
   [edd.request-cache :as request-cache]
   [lambda.request :as request]
   [edd.search :as search]
   [lambda.util :as util]))

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
         (assoc agr
                :version (:event-seq event)
                :id (:id event)))))
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
  [ctx {:keys [id events snapshot]}]
  {:pre [id events]}
  (log/debug "Updating aggregates" id)
  (log/debug "Events: " events)
  (log/debug "Snapshot: " snapshot)

  (cond
    (:error events) (throw (ex-info "Error fetching events" {:error events}))
    (> (count events) 0) (util/d-time
                          (format "Rebuilding aggregate state, id: %s, events: %s" id (count events))
                          (let [aggregate (create-aggregate snapshot events (:def-apply ctx))
                                result-agg (apply-agg-filter ctx aggregate)]
                            result-agg))
    :else (or snapshot
              nil)))

(defn fetch-snapshot
  [ctx id]
  (if-let [snapshot
           (util/d-time
            "Fetching snapshot"
            (search/get-snapshot ctx id))]
    (do (log/info "Found snapshot version: " (:version snapshot))
        snapshot)
    (log/info "Snapshot not found")))

(defn get-by-id
  [ctx id]
  {:pre [id]}
  (if-let [cache-snapshot (request-cache/get-aggregate ctx id)]
    (do
      (log/info (format "Found in cache, id: %s, :version %s"
                        (:id cache-snapshot)
                        (:version cache-snapshot)))
      cache-snapshot)
    (let [snapshot (fetch-snapshot ctx id)
          events (dal/get-events (assoc ctx
                                        :id id
                                        :version (:version snapshot)))
          _ (log/info (format "Events to apply: %s" (count events)))
          aggreagate (get-current-state ctx {:id id
                                             :events events
                                             :snapshot snapshot})]
      (request-cache/update-aggregate ctx aggreagate)
      aggreagate)))

(defn update-aggregate
  [ctx aggregate]
  (if aggregate
    (search/update-aggregate (assoc ctx
                                    :aggregate aggregate))
    ctx))

(defn handle-event
  [{:keys [apply] :as ctx}]
  (let [meta (:meta apply)
        ctx (assoc ctx :meta meta)
        realm (:realm meta)
        agg-id (:aggregate-id apply)
        request-scoped (request/is-scoped)
        applied (and request-scoped
                     (get-in @request/*request* [:applied realm agg-id]))]
    (util/d-time
     (format "handling-apply, id: %s, version: %s" realm (:aggregate-id apply))

     (when-not request-scoped
       ; Always update aggregate when no caching
       (update-aggregate ctx
                         (get-by-id ctx agg-id)))

     (when-not applied
       ; Cache mismatsh
       (update-aggregate ctx
                         (get-by-id ctx agg-id)))

     (when request-scoped
       ; Update cache if enabled
       (swap! request/*request*
              #(assoc-in % [:applied realm agg-id] {:apply true})))
     {:apply true})))
