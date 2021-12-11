(ns edd.el.cmd
  (:require
   [edd.flow :refer :all]
   [clojure.tools.logging :as log]
   [lambda.util :as util]
   [lambda.http-client :as http-client]
   [edd.dal :as dal]
   [lambda.request :as request]
   [edd.response.cache :as response-cache]
   [edd.el.query :as query]
   [malli.core :as m]
   [malli.error :as me]
   [edd.response.s3 :as s3-cache]
   [edd.ctx :as edd-ctx]
   [edd.el.ctx :as el-ctx]
   [edd.common :as common]
   [edd.el.event :as event]
   [edd.request-cache :as request-cache]
   [edd.schema.core :as edd-schema]
   [aws.aws :as aws])
  (:import (clojure.lang ExceptionInfo)))

(defn calc-service-url
  [service]
  (str "https://"
       (name
        service)
       "."
       (util/get-env "PrivateHostedZoneName")
       "/query"))

(defn call-query-fn
  [ctx cmd query-fn deps]
  (when (some
         #(contains? cmd %)
         (keys deps))
    (throw (ex-info "Duplicate key in deps and cmd"
                    {:message "Duplicate key in deps and cmd"
                     :error (filter
                             #(contains? cmd %)
                             (keys deps))})))
  (apply query-fn [(merge cmd deps)]))

(defn resolve-remote-dependency
  [ctx cmd {:keys [service query]} deps]
  (log/info "Resolving remote dependency: " service (:cmd-id cmd))

  (let [query-fn query
        url (calc-service-url
             service)
        token (aws/get-token ctx)
        response (http-client/retry-n
                  #(util/http-post
                    url
                    (http-client/request->with-timeouts
                     %
                     {:body    (util/to-json
                                {:query          (call-query-fn ctx cmd query-fn deps)
                                 :meta           (:meta ctx)
                                 :request-id     (:request-id ctx)
                                 :interaction-id (:interaction-id ctx)})
                      :headers {"Content-Type"    "application/json"
                                "X-Authorization" token}}
                     :idle-timeout 10000))
                  :retries 3)]
    (when (:error response)
      (throw (ex-info (str "Deps request error for " service) {:error (:error response)})))
    (when (:error (get response :body))
      (throw (ex-info (str "Deps request error for " service) (get response :body))))
    (if (> (:status response 0) 299)
      (throw (ex-info (str "Deps request error for " service) {:status (:status response)}))
      (get-in response [:body :result]))))

(defn resolve-local-dependency
  [ctx cmd query-fn deps]
  (log/debug "Resolving local dependency")
  (let [query (call-query-fn ctx cmd query-fn deps)]
    (when query
      (let [resp (query/handle-query ctx {:query query})]
        (if (:error resp)
          (throw (ex-info "Failed to resole local deps" {:error resp}))
          resp)))))

(defn fetch-dependencies-for-command
  [ctx {:keys [cmd-id] :as cmd}]
  (let [{:keys [deps]} (edd-ctx/get-cmd ctx cmd-id)
        deps (if (vector? deps)
               (partition 2 deps)
               deps)
        dps-value (reduce
                   (fn [p [key req]]
                     (log/info "Query for dependency" key)
                     (let [dep-value
                           (try (if (:service req)
                                  (resolve-remote-dependency
                                   ctx
                                   cmd
                                   req
                                   p)
                                  (resolve-local-dependency
                                   ctx
                                   cmd
                                   req
                                   p))
                                (catch AssertionError e
                                  (log/warn "Assertion error for deps " key)
                                  nil))]
                       (if dep-value
                         (assoc p key dep-value)
                         p)))
                   {}
                   deps)]
    (log/debug "Fetching context for" cmd-id deps)
    dps-value))

(defn with-breadcrumbs
  [ctx resp]
  (let [parent-breadcrumb (or (get-in ctx [:breadcrumbs]) [])]
    (update resp :effects
            (fn [cmds]
              (vec
               (map-indexed
                (fn [i cmd]
                  (assoc cmd :breadcrumbs
                         (conj parent-breadcrumb i))) cmds))))))

(defn wrap-commands
  [ctx commands]
  (if-not (contains? (into [] commands) :commands)
    (into []
          (map
           (fn [cmd]
             (if-not (contains? cmd :commands)
               {:service  (:service-name ctx)
                :commands [cmd]}
               cmd))
           (if (map? commands)
             [commands]
             commands)))
    [commands]))

(defn clean-effects
  [ctx effects]
  (->> effects
       (remove nil?)
       (wrap-commands ctx)))

(defn handle-effects
  [ctx & {:keys [resp aggregate]}]
  (let [events (:events resp)
        ctx (el-ctx/put-aggregate ctx aggregate)
        effects (reduce
                 (fn [cmd fx-fn]
                   (let [resp (fx-fn ctx events)]
                     (into cmd
                           (if (map? resp)
                             [resp]
                             resp))))
                 []
                 (:fx ctx))
        effects (flatten effects)
        effects (clean-effects ctx effects)
        effects (map #(assoc %
                             :request-id (:request-id ctx)
                             :interaction-id (:interaction-id ctx)
                             :meta (:meta ctx {}))
                     effects)]

    (assoc resp :effects effects)))

(defn to-clean-vector
  [resp]
  (if (or (vector? resp)
          (list? resp))
    (remove nil?
            resp)
    (if resp
      [resp]
      [])))

(defn resp->add-meta-to-events
  [ctx {:keys [events] :as resp}]
  (assoc resp
         :events
         (map
          (fn [{:keys [error] :as %}]
            (if-not error
              (assoc % :meta (:meta ctx {})
                     :request-id (:request-id ctx)
                     :interaction-id (:interaction-id ctx))
              %))
          events)))

(defn resp->add-user-to-events
  [{:keys [user] :as ctx} resp]
  (if-not user
    resp
    (update resp :events
            (fn
              [events]
              (map #(assoc %
                           :user (:id user)
                           :role (:role user))
                   events)))))

(defn resolve-command-id-with-id-fn
  "Resolving command id. Taking into account override function of id.
  If id-fn returns null we fallback to command id. Override should
  be only used when it is impossible to create id on client. Like in case
  of import"
  [ctx cmd]
  (let [cmd-id (:cmd-id cmd)
        {:keys [id-fn]} (edd-ctx/get-cmd ctx cmd-id)]

    (if id-fn
      (let [new-id (id-fn ctx cmd)]
        (if new-id
          (assoc cmd :id new-id)
          cmd))
      cmd)))

(defn resp->assign-event-seq
  [ctx {:keys [events] :as resp}]
  (let [aggregate (el-ctx/get-aggregate ctx)
        last-sequence (:version aggregate 0)]
    (assoc resp
           :events (map-indexed (fn [idx event]
                                  (assoc event
                                         :event-seq
                                         (+ last-sequence idx 1)))
                                events))))

(defn verify-command-version
  [ctx cmd]
  (let [aggregate (el-ctx/get-aggregate ctx)
        {:keys [version]} cmd
        current-version (:version aggregate)]
    (cond
      (nil? version) ctx
      (not= version current-version) (throw (ex-info "Wrong version"
                                                     {:current current-version
                                                      :version version}))
      :else ctx)))

(defn invoke-handler
  "We add try catch here in order to parse
  all Exceptions thrown by handler in to data. Afterwards we want only ex-info"
  [handler cmd ctx]
  (try
    (handler ctx cmd)
    (catch Exception e
      (let [data (ex-data e)]
        (when data
          (throw e))
        (log/warn "Command handler failed" e)
        (throw (ex-info "Command handler failed"
                        {:message "Command handler failed"}))))))

(defn get-response-from-command-handler
  [ctx & {:keys [command-handler cmd]}]
  (verify-command-version ctx cmd)
  (let [events (->> ctx
                    (invoke-handler command-handler cmd)
                    (to-clean-vector)
                    (map #(assoc % :id (:id cmd)))
                    (remove nil?))]
    (when (some :error events)
      (throw (ex-info "Invalid aggregate state"
                      {:error (filter :error events)})))
    (reduce
     (fn [p event]
       (cond-> p
         (contains? event :error) (update :events conj event)
         (contains? event :identity) (update :identities conj event)
         (contains? event :sequence) (update :sequences conj event)
         (contains? event :event-id) (update :events conj event)))
     {:events     []
      :identities []
      :sequences  []}
     events)))

(defn handle-command
  [ctx {:keys [cmd-id] :as cmd}]
  (log/info (:meta ctx))
  (util/d-time
   (str "handling-command-" cmd-id)
   (let [{:keys [handler]} (edd-ctx/get-cmd ctx cmd-id)
         dependencies (fetch-dependencies-for-command ctx cmd)
         ctx (merge dependencies ctx)
         {:keys [id] :as cmd} (resolve-command-id-with-id-fn ctx cmd)]
     (when-not handler
       (throw (ex-info "Missing command handler"
                       {:cmd-id (:cmd-id cmd)})))
     (let [aggregate (common/get-by-id ctx {:id id})
           ctx (el-ctx/put-aggregate ctx aggregate)
           resp (->> (get-response-from-command-handler
                      ctx
                      :cmd cmd
                      :command-handler handler)
                     (resp->add-user-to-events ctx)
                     (resp->assign-event-seq ctx))
           resp (assoc resp :meta [{cmd-id {:id id}}])
           aggregate-schema (edd-ctx/get-service-schema ctx)
           aggregate (-> ctx
                         (assoc :id id
                                :events (:events resp [])
                                :snapshot aggregate)
                         (event/get-current-state)
                         (:aggregate)
                         (or {}))]

       (when-not (m/validate aggregate-schema aggregate)
         (throw (ex-info "Invalid aggregate state"
                         {:error (me/humanize
                                  (m/explain aggregate-schema aggregate))})))

       (let [resp (handle-effects ctx
                                  :resp resp
                                  :aggregate aggregate)

             resp (resp->add-meta-to-events ctx resp)]
         (request-cache/update-aggregate ctx aggregate)
         resp)))))

(def initial-response
  {:meta       []
   :events     []
   :effects    []
   :sequences  []
   :identities []})

(defn validate-single-command
  [ctx {:keys [cmd-id] :as cmd}]
  (let [{:keys [consumes
                handler]} (edd-ctx/get-cmd ctx cmd-id)]
    (cond
      (not (m/validate (edd-schema/EddCoreCommand) cmd))
      {:error (->> cmd
                   (m/explain (edd-schema/EddCoreCommand))
                   (me/humanize))}
      (not handler)
      {:error (str "Missing handler: " cmd-id)}
      (not (m/validate consumes cmd))
      {:error (->> cmd
                   (m/explain consumes)
                   (me/humanize))}

      :else true)))

(defn validate-commands
  "Validate if commands match spec and if they are valid commands"
  [ctx commands]
  (when (empty? commands)
    (throw (ex-info "No commands present"
                    {:error "No commands present in request"})))
  (let [validations (mapv
                     (partial validate-single-command ctx)
                     commands)]
    (when (some :error validations)
      (throw (ex-info "Command schema validation failed"
                      {:error (mapv #(get % :error :valid) validations)})))))

(defn resp->response-summary
  [{:keys [no-summary]} resp]

  (cond
    no-summary resp :else {:success    true
                           :effects    (reduce
                                        (fn [p v]
                                          (concat
                                           p
                                           (map
                                            (fn [%] {:id           (:id %)
                                                     :cmd-id       (:cmd-id %)
                                                     :service-name (:service v)})
                                            (:commands v))))
                                        []
                                        (:effects resp))
                           :events     (count (:events resp))
                           :meta       (:meta resp)
                           :identities (count (:identities resp))
                           :sequences  (count (:sequences resp))}))

(defn retry [f n]
  (try
    (f)
    (catch ExceptionInfo e
      (let [data (ex-data e)]
        (request-cache/clear)
        (if (and (= (get-in data [:error :key])
                    :concurrent-modification)
                 (not (zero? n)))
          (do
            (log/warn (str "Failed handling attempt: " n) e)
            (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry f (dec n)))
          (throw e))))))

(defn- log-request [ctx body]
  (dal/log-request ctx body)
  ctx)

(defn store-results
  [ctx resp]
  (let [cache-result (response-cache/cache-response ctx resp)]
    (when (:error cache-result)
      (throw (ex-info "Error caching result" (:error cache-result))))
    (dal/store-results (assoc ctx :resp resp))
    (swap! request/*request*
           update
           :cache-keys
           (fn [v]
             (conj v cache-result)))
    resp))

(defn process-commands
  [ctx {:keys [commands] :as body}]
  (let [ctx (-> ctx
                (assoc :commands commands
                       :breadcrumbs (or (get body :breadcrumbs)
                                        [0]))
                (s3-cache/register))]

    (log-request ctx body)

    (if-let [resp (:data (dal/get-command-response ctx))]
      resp
      (do
        (validate-commands ctx commands)
        (let [resp (map
                    #(handle-command ctx %)
                    commands)
              resp (reduce
                    #(merge-with concat %1 %2)
                    initial-response
                    resp)
              resp (with-breadcrumbs ctx resp)
              resp (assoc resp
                          :summary (resp->response-summary ctx resp))
              resp (store-results ctx resp)]
          (:summary resp))))))

(defn handle-commands
  [ctx body]
  (let [ctx (assoc ctx
                   :meta (merge
                          (:meta ctx {})
                          (:meta body {})))
        ctx (s3-cache/register ctx)]

    (try
      (retry #(process-commands ctx body)
             3)
      (catch ExceptionInfo e
        (dal/log-request-error ctx body (ex-data e))
        (throw e)))))
