(ns edd.el.cmd
  (:require
    [edd.flow :refer :all]
    [clojure.tools.logging :as log]
    [lambda.util :as util]
    [edd.dal :as dal]
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
      (log/info "Deps request error for " service (:error response)))
    (when (:error (get response :body))
      (log/info "Deps result error for " service (get response :body)))
    (get-in response [:body :result])))

(defn resolve-local-dependency
  [ctx cmd query-fn]
  (log/debug "Resolving local dependency")
  (query/handle-query ctx {:query (call-query-fn ctx cmd query-fn)}))

(defn fetch-dependencies-for-command
  [ctx cmd]
  (let [cmd-id (:cmd-id cmd)
        dps (let [ctx-dps (get-in ctx [:dps cmd-id])]
              (if (vector? ctx-dps)
                (partition 2 ctx-dps)
                ctx-dps))
        dps-value (reduce
                    (fn [p [key req]]
                      (log/debug "Query for dependency" key req)
                      (let [dep-value
                            (if (:service req)
                              (resolve-remote-dependency
                                (assoc ctx :dps-resolved p)
                                cmd
                                req)
                              (resolve-local-dependency
                                (assoc ctx :dps-resolved p)
                                cmd
                                req))]
                        (if dep-value
                          (assoc p key dep-value)
                          p)))
                    {}
                    dps)]
    (log/debug "Fetching context for" cmd-id dps)
    (update ctx :dps-resolved #(conj % dps-value))))

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
        effects (flatten effects)]
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



(defn handle-command
  [{:keys [idx cmd command-handlers] :as ctx}]
  (log/debug "Handling individual command" (:cmd-id cmd) cmd)
  (let [cmd-id (:cmd-id cmd)
        command-handler (get command-handlers cmd-id (fn [_ _]
                                                       {:error :handler-no-found}))
        resp (->> ctx
                  (merge (get-in ctx [:dps-resolved idx]))
                  (#(command-handler % cmd))
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
                     :sequences  []}))]
    (-> ctx
        (assoc :resp resp)
        (add-user-to-events)
        (handle-effects)
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

(defn assign-events-seq
  [{:keys [resp] :as ctx}]
  (assoc-in
    ctx
    [:resp :events]
    (loop [c ctx
           events (get resp :events '())
           result []]
      (if (empty? events)
        result
        (let [event (first events)
              id (:id event)
              current-seq (get-in c [:last-event-seq id] 0)
              new-seq (inc current-seq)]
          (recur
            (assoc-in c [:last-event-seq id] new-seq)
            (rest events)
            (conj
              result
              (assoc event :event-seq new-seq)
              )))))))

(defn verify-command-version
  [{:keys [commands] :as ctx}]
  (assoc ctx :cmd-version
             (reduce
               (fn [p {:keys [version id]}]
                 (let [current-version (get-in ctx [:last-event-seq id])]
                   (cond
                     (nil? version) p
                     (> version (inc current-version)) (throw (ex-info "Wrong version"
                                                                       {:current current-version
                                                                        :version version}))
                     :else (assoc p [:last-event-seq id] version))))
               ctx
               commands)))

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
      {:events     []
       :commands   []
       :sequences  []
       :identities []}
      (vec commands))))


(defn resolve-command-id
  "Resolving command id. Taking into account override function of id.
  If Id in override returns null we fallback to command id. Override should
  be only used when it is impossible to create id on client. Like in case
  of import"
  [ctx cmd idx]
  (let [cmd-id (:cmd-id cmd)
        id-fn (get-in ctx [:id-fn cmd-id])]

    (if id-fn
      (let [new-id
            (id-fn
              (merge ctx
                     (get-in ctx [:dps-resolved idx]))
              cmd)]
        (if new-id
          (assoc cmd :id new-id
                     :original-id (:id cmd))
          cmd))
      cmd)))

(defn resolve-commands-id-fn
  [{:keys [commands] :as ctx}]
  (assoc ctx
    :commands
    (map-indexed
      (fn [idx cmd] (resolve-command-id ctx cmd idx))
      commands)))



(defn fetch-event-sequences-for-commands
  [{:keys [commands] :as ctx}]
  (reduce
    (fn [v cmd]
      (let [cmd-id (:cmd-id cmd)
            id (:id cmd)]
        (log/debug "Fetching sequences for" cmd-id id)
        (assoc-in v [:last-event-seq id]
                  (dal/get-max-event-seq
                    (assoc v :id id)))))
    ctx
    commands))

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
  [{:keys [resp commands] :as ctx}]
  (assoc-in ctx
            [:resp :meta]
            (mapv
              (fn [cmd]
                {(:cmd-id cmd) {:id (:id cmd)}})
              commands)))

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
  [{:keys [resp]}]
  (if-not (:error resp)
    {:success    true
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
     :sequences  (count (:sequences resp))}
    {:error (:error resp)}))

(defn check-for-errors
  [{:keys [resp] :as ctx}]
  (let [events (:events resp)
        errors (filter #(contains? % :error) events)]
    (if-not (empty? errors)
      (assoc ctx :error errors)
      ctx)))

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
                    (wrap-commands ctx)))))

(defn handle-commands
  [ctx body]
  (let [resp (e-> (assoc ctx :commands (:commands body))
                  (validate-commands)
                  (resolve-dependencies-to-context)
                  (resolve-commands-id-fn)
                  (fetch-event-sequences-for-commands)
                  (get-command-response)
                  (check-for-errors)
                  (assign-events-seq)
                  (verify-command-version)
                  (clean-effects)
                  (dal/store-results)
                  (add-metadata)
                  (set-response-summary))]
    (if (:error resp)
      (select-keys resp [:error])
      resp)))
