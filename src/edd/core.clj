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
            [edd.search :as search]
            [edd.ctx :as edd-ctx]))

(def EddCoreRegCmd
  (m/schema
   [:map
    [:handler [:fn fn?]]
    [:id-fn [:fn fn?]]
    [:deps [:or
            [:map]
            [:vector :any]]]
    [:consumes
     [:fn #(m/schema? (m/schema %))]]]))

(defn reg-cmd
  [ctx cmd-id reg-fn & rest]
  (log/debug "Registering cmd" cmd-id)
  (let [input-options (reduce
                       (fn [c [k v]]
                         (assoc c k v))
                       {}
                       (partition 2 rest))
        options input-options
        ; For compatibility
        options (-> options
                    (dissoc :spec)
                    (assoc :consumes (:spec options
                                            (:consumes options))))
        options (-> options
                    (assoc :id-fn (:id-fn options
                                          (fn [_ _] nil))))
        ; For compatibility
        options (-> options
                    (dissoc :dps)
                    (assoc :deps (:dps options
                                       (:deps options {}))))
        options (update options
                        :consumes
                        #(s/merge-cmd-schema % cmd-id))
        options (assoc options :handler (when reg-fn
                                          (fn [& rest]
                                            (apply reg-fn rest))))]

    (when (:dps input-options)
      (log/warn ":dps is deprecated and will be removed in future"))
    (when (:spec input-options)
      (log/warn ":spec is deprecated and will be removed in future"))

    (when-not (m/validate EddCoreRegCmd options)
      (throw (ex-info "Invalid command registration"
                      {:explain (-> (m/explain EddCoreRegCmd options)
                                    (me/humanize))})))
    (edd-ctx/put-cmd ctx
                     :cmd-id cmd-id
                     :options options)))

(defn reg-event
  [ctx event-id reg-fn]
  (log/debug "Registering apply" event-id)
  (update ctx :def-apply
          #(assoc % event-id (when reg-fn
                               (fn [& rest]
                                 (apply reg-fn rest))))))

(defn reg-agg-filter
  [ctx reg-fn]
  (log/debug "Registering aggregate filter")
  (assoc ctx :agg-filter
         (conj
          (get ctx :agg-filter [])
          (when reg-fn
            (fn [& rest]
              (apply reg-fn rest))))))

(def EddCoreRegQuery
  (m/schema
   [:map
    [:handler [:fn fn?]]
    [:produces
     [:fn #(m/schema? (m/schema %))]]
    [:consumes
     [:fn #(m/schema? (m/schema %))]]]))

(defn reg-query
  [ctx query-id reg-fn & rest]
  (log/debug "Registering query" query-id)
  (let [options (reduce
                 (fn [c [k v]]
                   (assoc c k v))
                 {}
                 (partition 2 rest))
        options (update options
                        :consumes
                        #(s/merge-query-consumes-schema % query-id))
        options (update options
                        :produces
                        #(s/merge-query-produces-schema %))
        options (assoc options :handler (when reg-fn
                                          (fn [& rest]
                                            (apply reg-fn rest))))]
    (when-not (m/validate EddCoreRegQuery options)
      (throw (ex-info "Invalid query registration"
                      {:explain (-> (m/explain EddCoreRegQuery options)
                                    (me/humanize))})))
    (assoc-in ctx [:edd-core :queries query-id] options)))

(defn reg-fx
  [ctx reg-fn]
  (update ctx :fx
          #(conj % (when reg-fn
                     (fn [& rest]
                       (apply reg-fn rest))))))

(defn event-fx-handler
  [ctx events]
  (mapv
   (fn [event]
     (let [handler (get-in ctx [:event-fx (:event-id event)])]
       (if handler
         (apply handler [ctx event])
         [])))
   events))

(defn reg-event-fx
  [ctx event-id reg-fn]
  (let [ctx (if (:event-fx ctx)
              ctx
              (reg-fx ctx event-fx-handler))]
    (update ctx
            :event-fx
            #(assoc % event-id (fn [& rest]
                                 (apply reg-fn rest))))))

(defn reg-service-schema
  "Register a service schema that will be serialised and returned when
  requested."
  [ctx schema]
  (assoc-in ctx [:edd-core :service-schema] schema))

(defn get-meta
  [ctx item]
  (merge
   (:meta ctx {})
   (:meta item {})))

(defn- add-log-level
  [attrs ctx item]
  (if-let [level (:log-level (get-meta ctx item))]
    (assoc attrs :log-level level)
    attrs))

(defn update-mdc-for-request
  [ctx item]
  (swap! request/*request* #(update % :mdc
                                    (fn [mdc]
                                      (-> (assoc mdc
                                                 :invocation-id (:invocation-id ctx)
                                                 :realm (:realm (get-meta ctx item))
                                                 :request-id (:request-id item)
                                                 :interaction-id (:interaction-id item))
                                          (add-log-level ctx item))))))

(defn dispatch-item
  [{:keys [item] :as ctx}]
  (log/debug "Dispatching" item)
  (update-mdc-for-request ctx item)
  (let [ctx (assoc ctx :request-id (:request-id item)
                   :interaction-id (:interaction-id item))]
    (try
      (let [item (if (contains? item :command)
                   (-> item
                       (assoc :commands [(:command item)])
                       (dissoc :command))
                   item)
            resp (cond
                   (contains? item :apply) (event/handle-event (-> ctx
                                                                   (assoc :apply (assoc
                                                                                  (:apply item)
                                                                                  :meta (get-meta ctx item)))))
                   (contains? item :query) (-> ctx
                                               (query/handle-query item))
                   (contains? item :commands) (-> ctx
                                                  (cmd/handle-commands item))
                   (contains? item :error) item
                   :else (do
                           (log/warn item)
                           {:error :invalid-request}))]
        (if (:error resp)
          {:error          (:error resp)
           :invocation-id  (:invocation-id ctx)
           :request-id     (:request-id item)
           :interaction-id (:interaction-id ctx)}
          {:result         resp
           :invocation-id  (:invocation-id ctx)
           :request-id     (:request-id item)
           :interaction-id (:interaction-id ctx)}))

      (catch Exception e
        (do
          (log/error e)
          (let [data (ex-data e)]
            (cond
              (:error data) (assoc data
                                   :invocation-id (:invocation-id ctx)
                                   :request-id (:request-id item)
                                   :interaction-id (:interaction-id ctx))
              data {:error          data
                    :invocation-id  (:invocation-id ctx)
                    :request-id     (:request-id item)
                    :interaction-id (:interaction-id ctx)}
              :else {:error "Unknown error processing item"})))))))

(defn dispatch-request
  [{:keys [body] :as ctx}]
  (util/d-time
   "Dispatching"
   (assoc
    ctx
    :resp (mapv
           #(dispatch-item (assoc ctx :item %))
           body))))

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
     [:command [:map]]]
    [:map
     [:commands sequential?]]
    [:map
     [:apply map?]]
    [:map
     [:query map?]]]])

(defn validate-request
  [{:keys [body] :as ctx}]
  (log/info "Validating request")
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
  (if (:skip body)
    (do (log/info "Skipping request")
        {})
    (with-stores
      ctx
      #(-> (assoc % :body body)
           (filter-queue-request)
           (validate-request)
           (dispatch-request)
           (prepare-response)))))
