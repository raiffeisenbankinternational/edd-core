(ns edd.core
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [edd.db :as db]
            [edd.el.cmd :as cmd]
            [edd.schema :as s]
            [edd.el.event :as event]
            [edd.el.query :as query]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [malli.util :as mu]
            [malli.error :as me]
            [malli.core :as m]
            [edd.dal :as dal]
            [edd.search :as search]))

(defn reg-cmd
  [ctx cmd-id reg-fn & {:keys [dps id-fn spec]}]
  (log/info "Registering cmd" cmd-id)
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
  (log/info "Registering apply" event-id)
  (update ctx :def-apply
          #(assoc % event-id reg-fn)))

(defn reg-agg-filter
  [ctx reg-fn]
  (log/info "Registering aggregate filter")
  (assoc ctx :agg-filter
             (conj
               (get ctx :agg-filter [])
               reg-fn)))

(defn reg-query
  [ctx query-id reg-fn & {:keys [spec]}]
  (log/info "Registering query" query-id)
  (-> ctx
      (update :query #(assoc % query-id reg-fn))
      (update :spec #(assoc % query-id (s/merge-query-schema spec)))))

(defn reg-fx
  [ctx reg-fn]
  (update ctx :fx
          #(conj % reg-fn)))



(defn dispatch-request
  [{:keys [body] :as ctx}]
  (log/debug "Dispatching" body)
  (assoc
    ctx
    :resp (cond
            (contains? body :apply) (event/handle-event (-> ctx
                                                            (assoc :apply (:apply body))))
            (contains? body :query) (query/handle-query
                                      ctx
                                      body)
            (contains? body :commands) (cmd/handle-commands
                                         ctx
                                         body)
            (contains? body :error) body
            :else {:error :invalid-request})))

(defn filter-queue-request
  "If request is coming from queue we need to get out all request bodies"
  [{:keys [body] :as ctx}]
  (if (contains? body :Records)
    (let [queue-body (-> body
                         (:Records)
                         (first)
                         (:body)
                         (util/to-edn))]
      (-> ctx
          (assoc :body queue-body)))

    ctx))

(defn prepare-response
  "Wrap non error result into :result keyword"
  [{:keys [resp] :as ctx}]
  (let [wrapped-resp (if-not (:error resp)
                       {:result resp}
                       resp)]
    (assoc ctx
      :resp (-> wrapped-resp
                (assoc
                  :request-id (:request-id ctx)
                  :interaction-id (:interaction-id ctx))))))

(defn prepare-request
  [{:keys [body] :as ctx}]
  (-> ctx
      (assoc :request-id (:request-id body)
             :interaction-id (:interaction-id body))))

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
  (if (m/validate schema body)
    ctx
    (assoc ctx
      :body {:error (->> body
                         (m/explain schema)
                         (me/humanize))})))

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
         (prepare-request)
         (dispatch-request)
         (prepare-response)
         (:resp))))
