(ns edd.el.cmd
  (:require
   [edd.flow :refer :all]
   [clojure.tools.logging :as log]
   [lambda.util :as util]
   [edd.dal :as dal]
   [lambda.request :as request]
   [edd.cache :as cache]
   [edd.schema :as s]
   [edd.el.query :as query]
   [malli.core :as m]
   [aws :as aws]
   [malli.error :as me]
   [malli.util :as mu]))

(defn calc-service-url
  [service]
  (str "https://"
       (name
        service)
       "."
       (util/get-env "PrivateHostedZoneName")
       "/query"))

(defn call-query-fn
  [ctx cmd query-fn]

  (apply query-fn [(merge cmd
                          (get-in ctx [:dps-resolved]))]))

(defn resolve-remote-dependency
  [ctx cmd {:keys [service query]}]
  (log/info "Resolving remote dependency: " service (:cmd-id cmd))

  (let [query-fn query
        url (calc-service-url
             service)
        token (aws/get-token ctx)
        response (util/http-post
                  url
                  {:timeout 10000
                   :body    (util/to-json
                             {:query          (call-query-fn ctx cmd query-fn)
                              :request-id     (:request-id ctx)
                              :interaction-id (:interaction-id ctx)})
                   :headers {"Content-Type"    "application/json"
                             "X-Authorization" token}})]
    (when (:error response)
      (log/error "Deps request error for " service (:error response)))
    (when (:error (get response :body))
      (log/error "Deps result error for " service (get response :body)))
    (get-in response [:body :result])))

(defn resolve-local-dependency
  [ctx cmd query-fn]
  (log/debug "Resolving local dependency")
  (let [query (call-query-fn ctx cmd query-fn)]
    (when query
      (query/handle-query ctx {:query query}))))

(defn fetch-dependencies-for-command
  [ctx cmd]
  (let [cmd-id (:cmd-id cmd)
        dps (let [ctx-dps (get-in ctx [:dps cmd-id])]
              (if (vector? ctx-dps)
                (partition 2 ctx-dps)
                ctx-dps))
        dps-value (reduce
                   (fn [p [key req]]
                     (log/info "Query for dependency" key)
                     (let [dep-value
                           (try (if (:service req)
                                  (resolve-remote-dependency
                                   (assoc ctx :dps-resolved p)
                                   cmd
                                   req)
                                  (resolve-local-dependency
                                   (assoc ctx :dps-resolved p)
                                   cmd
                                   req))
                                (catch AssertionError e
                                  nil))]
                       (if dep-value
                         (assoc p key dep-value)
                         p)))
                   {}
                   dps)]
    (log/debug "Fetching context for" cmd-id dps)
    (assoc ctx :dps-resolved dps-value)))

(defn handle-effects
  [{:keys [resp] :as ctx}]
  (let [events (:events resp)
        effects (reduce
                 (fn [cmd fx]
                   (let [resp (fx ctx events)]
                     (into cmd
                           (if (map? resp)
                             [resp]
                             resp))))
                 []
                 (:fx ctx))
        effects (->>
                 (flatten effects))]
    (assoc-in ctx [:resp :commands] effects)))

(defn to-clean-vector
  [resp]
  (if (or (vector? resp)
          (list? resp))
    (remove nil?
            resp)
    (if resp
      [resp]
      [])))

(defn add-user-to-events
  [{:keys [user] :as ctx}]
  (if-not user
    ctx
    (update-in
     ctx
     [:resp :events]
     (fn
       [events]
       (map #(assoc %
                    :user (:id user)
                    :role (:role user))
            events)))))

(defn resolve-command-id
  "Resolving command id. Taking into account override function of id.
  If Id in override returns null we fallback to command id. Override should
  be only used when it is impossible to create id on client. Like in case
  of import"
  [ctx cmd]
  (let [cmd-id (:cmd-id cmd)
        id-fn (get-in ctx [:id-fn cmd-id])]

    (if id-fn
      (let [new-id
            (id-fn
             (merge ctx
                    (get-in ctx [:dps-resolved]))
             cmd)]
        (if new-id
          (assoc cmd :id new-id
                 :original-id (:id cmd))
          cmd))
      cmd)))

(defn fetch-event-sequence-for-command
  [ctx cmd]
  (let [request (if (bound? #'request/*request*)
                  @request/*request*
                  {})
        cmd-id (:cmd-id cmd)
        id (:id cmd)
        last-seq (get-in request [:last-event-seq id])]
    (log/debug "Fetching sequences for" cmd-id id)
    (assoc-in ctx [:last-event-seq id]
              (if last-seq
                (do (log/debug "using last-event-seq for cmd" cmd last-seq)
                    last-seq)
                (do (log/debug "fetch version from store for cmd" cmd)
                    (dal/get-max-event-seq
                     (assoc ctx :id id)))))))

(defn verify-command-version
  [ctx {:keys [version id]}]
  (let [current-version (get-in ctx [:last-event-seq id])]
    (cond
      (nil? version) ctx
      (not= version current-version) (throw (ex-info "Wrong version"
                                                     {:current current-version
                                                      :version version}))
      :else ctx)))

(defn invoke-handler
  [handler cmd ctx]
  (try
    (handler ctx cmd)
    (catch Exception e
      (do (log/error e)
          {:error "Command handler failed"}))))

(defn handle-command
  [{:keys [cmd command-handlers] :as ctx}]

  (let [ctx (fetch-dependencies-for-command ctx cmd)
        cmd (resolve-command-id ctx cmd)
        cmd-id (:cmd-id cmd)
        ctx (cache/fetch-event-sequence-for-command ctx cmd)
        command-handler (get command-handlers cmd-id (fn [_ _]
                                                       {:error :handler-no-found}))
        dps-resolved (get-in ctx [:dps-resolved])
        ctx-with-dps (merge ctx dps-resolved)
        resp (->> ctx-with-dps
                  (#(verify-command-version % cmd))
                  (invoke-handler command-handler cmd)
                  (to-clean-vector)
                  (map #(assoc
                         %
                         :id (:id cmd)))
                  (remove nil?)
                  (reduce
                   (fn [p event]
                     (cond-> p
                       (contains? event :error) (update :events conj event)
                       (contains? event :identity) (update :identities conj event)
                       (contains? event :sequence) (update :sequences conj event)
                       (contains? event :event-id) (update :events conj event)))
                   {:events     []
                    :identities []
                    :sequences  []}))
        resp (assoc resp :meta {cmd-id (:id cmd)})]

    (-> ctx-with-dps
        (assoc :resp resp)
        (add-user-to-events)
        (handle-effects)
        (cache/track-intermediate-events!)
        (:resp))))

(defn handle-identities
  [ctx events]
  (let [ident (filter #(contains? % :identity) events)]
    (log/debug "Identity" ident)
    ident))

(defn handle-sequences
  [ctx events]
  (let [sequence (filter #(contains? % :sequence) events)]
    (log/debug "Sequence" sequence)
    sequence))

(defn get-command-response
  [{:keys [commands] :as ctx}]
  (assoc ctx
         :resp
         (reduce-kv
          (fn [p idx cmd]
            (merge-with
             concat
             p
             (handle-command (-> ctx
                                 (assoc :cmd cmd
                                        :idx idx)))))
          {:meta       []
           :events     []
           :commands   []
           :sequences  []
           :identities []}
          (vec commands))))

(defn resolve-dependencies-to-context
  [{:keys [commands] :as ctx}]
  (log/debug "Context preparation started" commands)
  (-> (reduce
       (fn [v cmd]
         (log/debug "Preparing context" cmd)
         (fetch-dependencies-for-command v cmd))
       (assoc ctx
              :dps-resolved [])
       commands)
      (dal/log-dps)))

(defn add-metadata
  [{:keys [resp] :as ctx}]
  (update-in ctx
             [:resp :meta]
             #(mapv (fn [[k v]] {k {:id v}}) %)))

(def default-command-schema
  [:map [:cmd-id keyword?]])

(defn validate-commands
  "Validate if commands match spec and if they are valid commands"
  [{:keys [commands] :as ctx}]
  (let [result (mapv
                (fn [{:keys [cmd-id] :as cmd}]
                  (let [schema (get-in ctx [:spec cmd-id]
                                       default-command-schema)
                        schema (mu/merge default-command-schema
                                         schema)
                        schema-valid (m/validate schema cmd)
                        cmd-exits (get-in ctx [:command-handlers cmd-id])]
                    (cond
                      (not schema-valid) (->> cmd
                                              (m/explain schema)
                                              (me/humanize))
                      (not cmd-exits) {:unknown-command-handler
                                       cmd-id}
                      :else :valid)))
                commands)]
    (cond-> ctx
      (empty? commands) (assoc-in [:error :commands] :empty)
      (some #(not= % :valid) result) (assoc-in [:error :spec] result))))

(defn set-response-summary
  [{:keys [resp no-summary] :as ctx}]
  (cond
    (:error resp) {:error (:error resp)}
    no-summary resp
    :else {:success    true
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
                        (:commands resp))
           :events     (count (:events resp))
           :meta       (:meta resp)
           :identities (count (:identities resp))
           :sequences  (count (:sequences resp))}))

(defn check-for-errors
  [{:keys [resp] :as ctx}]
  (let [events (:events resp)
        errors (filter #(contains? % :error) events)]
    (if-not (empty? errors)
      (assoc ctx :error errors)
      ctx)))

(defn with-breadcrumbs [ctx cmds]
  (let [parent-breadcrumb (or (get-in ctx [:breadcrumbs]) [])]
    (map-indexed (fn [i cmd] (assoc cmd :breadcrumbs (conj parent-breadcrumb i))) cmds)))

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
  [ctx]
  (update-in ctx [:resp :commands]
             (fn [effects]
               (->> effects
                    (remove nil?)
                    (wrap-commands ctx)
                    (with-breadcrumbs ctx)))))

(defn retry [f n]
  (let [response (f)]
    (if (and (= (get-in response [:error :key])
                :concurrent-modification)
             (not (zero? n)))
      (do
        (Thread/sleep (+ 1000 (rand-int 1000)))
        (cache/flush-cache)
        (retry f (dec n)))
      response)))

(defn versioned-events! [ctx]
  (assoc-in ctx [:resp :events] (get @request/*request* :events [])))

(defn- log-request [ctx body]
  (dal/log-request ctx body)
  ctx)

(defn add-response-summary [ctx]
  (assoc ctx :response-summary (set-response-summary ctx)))

(defn process-commands [ctx body]

  (let [ctx (assoc ctx :commands (:commands body)
                   :breadcrumbs (or (get body :breadcrumbs)
                                    [0]))]

    (log-request ctx body)
    (if-let [resp (:data (dal/get-command-response ctx))]
      resp
      (e-> ctx
           (validate-commands)
           (get-command-response)
           (check-for-errors)
           (clean-effects)
           (versioned-events!)
           (add-metadata)
           (add-response-summary)
           (dal/store-results)
           :response-summary))))

(defn handle-commands
  [ctx body]
  (cache/clear!)
  (let [resp (retry #(process-commands ctx body)
                    3)]
    (if (:error resp)
      (select-keys resp [:error])
      resp)))
