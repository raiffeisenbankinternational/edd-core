(ns edd.core
  (:require [clojure.tools.logging :as log]
            [lambda.request :as request]
            [edd.el.cmd :as cmd]
            [edd.schema :as s]
            [edd.el.event :as event]
            [edd.el.query :as query]
            [lambda.util :as util]
            [malli.error :as me]
            [malli.core :as m]
            [edd.dal :as dal]
            [edd.search :as search]))

(defn reg-cmd
  [ctx cmd-id reg-fn & {:keys [dps id-fn spec]}]
  (log/debug "Registering cmd" cmd-id)
  (let [new-ctx
        (-> ctx
            (update :command-handlers #(assoc % cmd-id reg-fn))
            (update :dps (if dps
                           #(assoc % cmd-id dps)
                           #(assoc % cmd-id [])))
            (update :spec #(assoc % cmd-id (s/merge-cmd-schema spec))))]
    (if id-fn
      (assoc-in new-ctx [:id-fn cmd-id] id-fn)
      new-ctx)))

(defn reg-event
  [ctx event-id reg-fn]
  (log/debug "Registering apply" event-id)
  (update ctx :def-apply
          #(assoc % event-id reg-fn)))

(defn reg-agg-filter
  [ctx reg-fn]
  (log/debug "Registering aggregate filter")
  (assoc ctx :agg-filter
         (conj
          (get ctx :agg-filter [])
          reg-fn)))

(defn reg-query
  [ctx query-id reg-fn & {:keys [spec]}]
  (log/debug "Registering query" query-id)
  (-> ctx
      (update :query #(assoc % query-id reg-fn))
      (update :spec #(assoc % query-id (s/merge-query-schema spec)))))

(defn reg-fx
  [ctx reg-fn]
  (update ctx :fx
          #(conj % reg-fn)))

(defn- add-log-level
  [attrs body]
  (if-let [level (:log-level body)]
    (assoc attrs :level level)
    attrs))

(defn dispatch-item
  [{:keys [item] :as ctx}]
  (log/debug "Dispatching" item)
  (swap! request/*request* #(update % :mdc
                                    (fn [mdc]
                                      (-> (assoc mdc
                                                 :invocation-id (:invocation-id ctx)
                                                 :request-id (:request-id item)
                                                 :interaction-id (:interaction-id item))
                                          (add-log-level item)))))
  (try
    (let [ctx (assoc ctx :request-id (:request-id item)
                     :interaction-id (:interaction-id item))
          resp (cond
                 (contains? item :apply) (event/handle-event (-> ctx
                                                                 (assoc :apply (:apply item))))
                 (contains? item :query) (-> ctx
                                             (query/handle-query item))
                 (contains? item :commands) (-> ctx
                                                (cmd/handle-commands item))
                 (contains? item :error) item
                 :else {:error :invalid-request})]
      (if (:error resp)
        {:error          (:error resp)
         :request-id     (:request-id item)
         :interaction-id (:interaction-id ctx)}
        {:result         resp
         :request-id     (:request-id item)
         :interaction-id (:interaction-id ctx)}))

    (catch Throwable e
      (do
        (log/error e)
        (throw e)))))

(defn dispatch-request
  [{:keys [body] :as ctx}]
  (log/debug "Dispatching" body)
  (assoc
   ctx
   :resp (mapv
          #(dispatch-item (assoc ctx :item %))
          body)))

(defn filter-queue-request
  "If request is coming from queue we need to get out all request bodies"
  [{:keys [body] :as ctx}]
  (if (contains? body :Records)
    (let [queue-body (mapv
                      (fn [it]
                        (-> it
                            (:body)
                            (util/to-edn)))
                      (-> body
                          (:Records)))]
      (-> ctx
          (assoc :body queue-body
                 :queue-request true)))

    (assoc ctx :body [body]
           :queue-request false)))

(defn prepare-response
  "Wrap non error result into :result keyword"
  [{:keys [resp] :as ctx}]
  (println resp)
  (if (:queue-request ctx)
    resp
    (first resp)))

(def schema
  [:and
   [:map
    [:request-id uuid?]
    [:interaction-id uuid?]]
   [:or
    [:map
     [:commands sequential?]]
    [:map
     [:apply map?]]
    [:map
     [:query map?]]]])

(defn validate-request
  [{:keys [body] :as ctx}]
  (assoc
   ctx
   body
   (mapv
    #(if (m/validate schema %)
       %
       {:error (->> body
                    (m/explain schema)
                    (me/humanize))})
    body)))

(defn with-stores
  [ctx body-fn]
  (search/with-init
    ctx
    #(dal/with-init
       % body-fn)))

(defn handler
  [ctx body]
  (log/debug "Handler body" body)
  (log/debug "Context" ctx)
  (with-stores
    ctx
    #(-> (assoc % :body body)
         (filter-queue-request)
         (validate-request)
         (dispatch-request)
         (prepare-response))))
