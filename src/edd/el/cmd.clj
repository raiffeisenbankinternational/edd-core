(ns edd.el.cmd
  (:require
   [edd.flow :refer :all]
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [lambda.util :as util]
   [edd.dal :as dal]
   [lambda.request :as request]
   [edd.response.cache :as response-cache]
   [malli.error :as me]
   [edd.response.s3 :as s3-cache]
   [edd.ctx :as edd-ctx]
   [edd.el.ctx :as el-ctx]
   [edd.common :as common]
   [edd.el.event :as event]
   [edd.request-cache :as request-cache]
   [edd.schema.core :as edd-schema]
   [edd.el.query :as query])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent Executors
                                 ExecutorService
                                 Future)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def resolve-remote-dependency query/resolve-remote-dependency)
(def resolve-local-dependency query/resolve-local-dependency)

(defn fetch-dependencies-for-command
  [ctx {:keys [cmd-id] :as cmd}]
  (log/debug "Fetching context for: " cmd-id)
  (util/d-time
   (str "Fetching all deps for " cmd-id)
   (let [{:keys [deps]} (edd-ctx/get-cmd ctx cmd-id)]
     (query/fetch-dependencies-for-deps ctx deps cmd))))

(defn with-breadcrumbs
  [ctx resp]
  (let [parent-breadcrumb (or (get-in ctx [:breadcrumbs]) [])]
    (update resp :effects
            (fn [cmds]
              (map-indexed
               (fn [i cmd]
                 (assoc cmd :breadcrumbs
                        (conj parent-breadcrumb i))) cmds)))))

(defn wrap-commands
  [ctx commands]
  (let [commands (if (map? commands)
                   [commands]
                   commands)]
    (map
     (fn [cmd]
       (let [cmd (if-not (:commands cmd)
                   {:commands [cmd]}
                   cmd)
             cmd (update cmd
                         :service
                         #(or %
                              (:service-name ctx)))]
         cmd))
     commands)))

(defn clean-effects
  [ctx effects]
  (if (seq effects)
    (->> effects
         (remove nil?)
         (wrap-commands ctx)
         (filter #(seq (:commands %))))
    []))

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
                             :meta (merge (:meta %) (:meta ctx {})))
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
  [{:keys [user]} resp]
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
        last-sequence (long (:version aggregate 0))]
    (assoc resp
           :events (map-indexed (fn [^long idx event]
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
                                                     {:error   :concurrent-modification
                                                      :message "Version mismatch"
                                                      :state   {:current current-version
                                                                :version version}}))
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
                    (remove nil?))
        response {:events     []
                  :identities []
                  :sequences  []}]
    (if (some :error events)
      (assoc response
             :error (vec
                     (filter :error events)))
      (reduce
       (fn [p event]
         (cond-> p
           (contains? event :identity) (update :identities conj event)
           (contains? event :sequence) (update :sequences conj event)
           (contains? event :event-id) (update :events conj event)))
       response
       events))))

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

(defn with-aggregate
  [aggregate]
  (when aggregate
    (swap! request/*request* #(update % :mdc
                                      assoc
                                      :aggregate
                                      (select-keys aggregate
                                                   [:id :version])))))

(defn handle-command
  [ctx {:keys [cmd-id] :as cmd}]
  (log/info "Requst meta: " (:meta ctx))
  (util/d-time
   (str "handling-command: " cmd-id)
   (let [{:keys [handler]} (edd-ctx/get-cmd ctx cmd-id)
         dependencies (fetch-dependencies-for-command ctx cmd)
         ctx (merge dependencies ctx)
         {:keys [id] :as cmd} (resolve-command-id-with-id-fn ctx cmd)
         {:keys [error]
          :as validation} (validate-single-command ctx cmd)]
     (when-not handler
       (throw (ex-info "Missing command handler"
                       {:cmd-id (:cmd-id cmd)})))
     (if error
       validation
       (let [aggregate (common/get-by-id ctx id)
             _ (with-aggregate aggregate)
             ctx (el-ctx/put-aggregate ctx aggregate)
             resp (->> (get-response-from-command-handler
                        ctx
                        :cmd cmd
                        :command-handler handler)
                       (resp->add-user-to-events ctx)
                       (resp->assign-event-seq ctx))
             resp (assoc resp :meta [{cmd-id {:id id}}])
             aggregate-schema (edd-ctx/get-service-schema ctx)
             aggregate (event/get-current-state
                        ctx
                        {:id id
                         :events (:events resp [])
                         :snapshot aggregate})
             _ (with-aggregate aggregate)]

         (when (and
                aggregate
                (not
                 (m/validate aggregate-schema aggregate)))
           (throw (ex-info "Invalid aggregate state"
                           {:error (me/humanize
                                    (m/explain aggregate-schema aggregate))})))

         (let [resp (handle-effects ctx
                                    :resp resp
                                    :aggregate aggregate)

               resp (resp->add-meta-to-events ctx resp)]
           (request-cache/update-aggregate ctx aggregate)
           (request-cache/store-identities ctx (:identities resp))
           resp))))))

(def initial-response
  {:meta       []
   :events     []
   :effects    []
   :sequences  []
   :identities []})

(defn validate-commands
  "Validate if commands match spec and if they are valid commands"
  [_ commands]
  (when (empty? commands)
    (throw (ex-info "No commands present"
                    {:error "No commands present in request"}))))

(defn resp->response-summary
  [{:keys [no-summary]} resp]

  (let [effects (:effects resp)
        effects (if (> (count effects) 100)
                  []
                  effects)]
    (cond
      (:error resp) resp
      no-summary resp :else {:success    true
                             :effects    (reduce
                                          (fn [p v]
                                            (into
                                             p
                                             (map
                                              (fn [%] {:id           (:id %)
                                                       :cmd-id       (:cmd-id %)
                                                       :service-name (:service v)})
                                              (:commands v))))
                                          []
                                          effects)
                             :events     (count (:events resp))
                             :meta       (:meta resp)
                             :identities (count (:identities resp))
                             :sequences  (count (:sequences resp))})))

(defn retry [f ^long n]
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
            (Thread/sleep (+ 1000 (long (rand-int 1000))))
            (retry f (dec n)))
          (throw e))))))

(defn- log-request [ctx body]
  (dal/log-request ctx body)
  ctx)

(defn resp->store-cache-partition
  [ctx resp]
  (response-cache/cache-response ctx resp))

(defn do-execute
  [jobs]
  (System/gc)
  (let [resonable-theread-count "10"
        param-threads (util/get-env "CacheThreadCount"
                                    resonable-theread-count)
        param-threads (Integer/parseInt param-threads)
        nthreads (min (count jobs)
                      param-threads)
        _ (log/info (str "Using threads: " nthreads
                         " to run: " (count jobs)
                         " jobs"))
        ^ExecutorService pool (Executors/newFixedThreadPool nthreads)
        resp (mapv
              (fn [^Future future]
                (.get future))
              (.invokeAll pool jobs))]
    (.shutdown pool)
    resp))

(defn resp->cache-partitioned
  [ctx resp]
  (let [effects (:effects resp)
        resp (dissoc resp :effects)
        partition-site (el-ctx/get-effect-partition-size ctx)]
    (if (< (long (count effects)) partition-site)
      (assoc resp
             :cache-result
             (resp->store-cache-partition ctx
                                          (assoc resp
                                                 :effects effects))
             :effects effects)
      (let [_ (System/gc)

            parts
            (partition partition-site partition-site nil effects)

            _ (System/gc)

            cache-result
            (do-execute
             (map-indexed
              (fn [idx e]
                #(resp->store-cache-partition ctx (assoc resp
                                                         :effects e
                                                         :idx idx)))
              parts))]

        (System/gc)
        (assoc resp
               :cache-result cache-result
               :effects (flatten parts))))))

(defn store-results
  [ctx resp]
  (let [{:keys [cache-result] :as resp}
        (resp->cache-partitioned ctx resp)

        resp
        (dissoc resp
                :cache-result)]
    (when (:error cache-result)
      (throw (ex-info "Error caching result" (:error cache-result))))
    (dal/store-results (assoc ctx
                              :resp
                              (assoc-in resp
                                        [:summary :cache-result]
                                        cache-result)))
    (swap! request/*request*
           update
           :cache-keys
           (fn [v]
             (conj v cache-result)))
    (:summary resp)))

(defn process-commands
  [ctx {:keys [commands] :as body}]
  (let [ctx (-> ctx
                (assoc :commands commands
                       :breadcrumbs (or (get body :breadcrumbs)
                                        [0]))
                (s3-cache/register))]

    (log-request ctx body)

    (if-let [resp (:data (dal/get-command-response ctx))]
      (do
        (swap! request/*request*
               update
               :cache-keys
               (fn [v]
                 (conj v (:cache-result resp))))
        (dissoc resp :cache-result))
      (do
        (validate-commands ctx commands)
        (let [resp (loop [queue commands
                          resp initial-response]
                     (let [c (first queue)
                           r (handle-command ctx c)
                           resp (merge-with concat resp r)]
                       (cond
                         (:error r) (assoc initial-response
                                           :error (:error r))
                         (seq
                          (rest queue)) (recur (rest queue)
                                               resp)
                         :else resp)))
              resp (with-breadcrumbs ctx resp)
              resp (assoc resp
                          :summary (resp->response-summary ctx resp))
              summary (store-results ctx resp)]
          summary)))))

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
