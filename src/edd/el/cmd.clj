(ns edd.el.cmd
  (:require
    [clojure.tools.logging :as log]
    [lambda.util :as util]
    [edd.dal :as dal]
    [edd.schema :as s]
    [edd.el.query :as query]
    [malli.core :as m]
    [malli.error :as me]))

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

(defn store-commands
  [ctx commands]
  (log/debug "Storing effects" commands)
  (let [clean-commands (->> commands
                            (remove nil?)
                            (wrap-commands ctx))]

    (doall
      (for [cmd clean-commands]
        (dal/store-cmd ctx (assoc
                             cmd
                             :request-id (:request-id ctx)
                             :interaction-id (:interaction-id ctx)))))
    clean-commands))

(defn store-events-in-realm
  [ctx realm events]
  (log/debug "Storing events in realm" (:id realm) (:name (:data realm)))
  (doall
    (for [event (flatten events)]
      (dal/store-event ctx realm event)))
  events)

(defn store-identities
  [ctx identities]
  (log/debug "Storing identities" identities)
  (doall
    (for [ident identities]
      (dal/store-identity ctx ident)))
  identities)

(defn store-sequences
  [ctx sequences]
  (log/debug "Storing sequences" sequences)
  (doall
    (for [sequence sequences]
      (dal/store-sequence ctx sequence)))
  sequences)

(defn persist-events
  [ctx resp]
  (let [realm (dal/read-realm ctx)]
    (let [response
          {:events     (store-events-in-realm
                         ctx
                         realm
                         (:events resp))

           :identities (if (> (count (:identities resp)) 0)
                         (store-identities ctx (:identities resp))
                         [])
           :commands   (store-commands
                         ctx
                         (:commands resp))}]
      response)))

(defn calc-service-url
  [service]
  (str "https://"
       (name
         service)
       "."
       (util/get-env "PrivateHostedZoneName")
       "/query"))

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
                               {:query          (query-fn cmd)
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
  (query/handle-query ctx {:query (query-fn cmd)}))

(defn fetch-dependencies-for-command
  [ctx cmd]
  (let [cmd-id (:cmd-id cmd)
        dps (cmd-id (:dps ctx))
        dps-value (reduce
                    (fn [p [key req]]
                      (log/debug "Query for dependency" key req)
                      (let [dep-value
                            (if (:service req)
                              (resolve-remote-dependency ctx cmd req)
                              (resolve-local-dependency ctx cmd req))]
                        (if dep-value
                          (assoc p key dep-value)
                          p)))
                    {}
                    dps)]
    (log/debug "Fetching context for" cmd-id dps)
    (update ctx :dps-resolved #(conj % dps-value))))

(defn handle-effects
  [ctx events]
  (reduce
    (fn [cmd fx]
      (let [resp (fx ctx events)]
        (into cmd
              (if (map? resp)
                [resp]
                resp))))
    []
    (:fx ctx)))

(defn to-clean-vector
  [resp]
  (if (or (vector? resp)
          (list? resp))
    (remove nil?
            resp)
    (if resp
      [resp]
      [])))

(defn handle-command
  [ctx cmd]
  (log/debug "Handling individual command" (:cmd-id cmd) cmd)
  (let [cmd-id (:cmd-id cmd)
        command-handler (cmd-id (:command-handlers ctx))]
    (log/debug "Discovered command handler" command-handler)
    (if command-handler
      (let [resp (command-handler
                   ctx
                   cmd)
            respond-list (to-clean-vector resp)
            assigned-id (map #(assoc
                                %
                                :id (:id cmd))
                             respond-list)]
        (log/debug "Handler response" resp)
        {:events   assigned-id
         :commands (handle-effects ctx (flatten assigned-id))}))))

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
  [ctx events]
  (if (empty? events)
    '()
    (let [event (first events)
          event-id (:id event)
          current-seq (get-in ctx [:last-event-seq event-id] 0)]
      (cons
        (assoc event :event-seq (+ current-seq 1))
        (assign-events-seq
          (assoc-in ctx [:last-event-seq event-id] (+ current-seq 1))
          (rest events))))))

(defn get-command-response
  [ctx cmd idx]
  (let [cmd-ctx (merge ctx
                       (get-in ctx [:dps-resolved idx]))]
    (->> cmd
         (handle-command cmd-ctx))))

(defn add-user-to-events
  [{:keys [user]} event]
  (if user
    (assoc event
      :user (:id user)
      :role (:role user))
    event))

(defn get-response
  [ctx body]
  (let [cmd-resp
        (map-indexed
          (fn [idx cmd]
            (get-command-response ctx cmd idx))
          (:commands body))
        response-events (flatten (map #(:events %) cmd-resp))
        cmd-identities (remove nil? (handle-identities ctx response-events))
        cmd-sequences (remove nil? (handle-sequences ctx response-events))]

    (log/debug "Response for all commands" cmd-resp)
    (let [error (first
                  (filter
                    #(contains? % :error)
                    response-events))
          only-events (remove
                        #(or (contains? % :identity)
                             (contains? % :sequence))
                        response-events)
          assigned-event-seq (vec
                               (assign-events-seq
                                 ctx
                                 only-events))]
      (if error
        error
        {:events     (mapv
                       (partial add-user-to-events ctx)
                       assigned-event-seq)
         :identities (into [] cmd-identities)
         :sequences  (into [] cmd-sequences)
         :commands   (into [] (flatten (map #(:commands %) cmd-resp)))}))))

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

(defn prepare-commands
  [ctx body]
  {:commands
   (map-indexed
     (fn [idx cmd] (resolve-command-id ctx cmd idx))
     (:commands body))})

(defn fetch-event-sequences-for-commands
  [ctx commands]
  (reduce
    (fn [v cmd]
      (let [cmd-id (:cmd-id cmd)
            id (:id cmd)]
        (log/debug "Fetching sequences for" cmd-id id)
        (assoc-in v [:last-event-seq id]
                  (dal/get-max-event-seq
                    v
                    id))))
    ctx
    (:commands commands)))

(defn resolve-dependencies-to-context
  [ctx body]
  (log/debug "Context preparation started" body)
  (-> (reduce
        (fn [v cmd]
          (log/debug "Preparing context" cmd)
          (fetch-dependencies-for-command v cmd))
        (assoc ctx
          :dps-resolved [])
        (:commands body))
      (dal/log-dps)))

(defn persist-sequences
  [ctx resp transaction-response]
  (if-not (:error transaction-response)
    (assoc transaction-response
      :sequences (if (> (count (:sequences resp)) 0)
                   (store-sequences ctx (:sequences resp))
                   []))
    transaction-response))

(defn add-metadata
  [ctx ready-body transaction-response]
  (assoc transaction-response
    :meta (reduce
            (fn [v cmd]
              (assoc v
                (:cmd-id cmd) {:id (:id cmd)}))
            {}
            (:commands ready-body))))

(defn validate-spec
  [ctx body]
  (reduce
    #(and %1 (m/validate (get-in ctx [:spec (:cmd-id %2)]) %2))
    true
    (:commands body)))

(defn explain-spec
  [ctx body]
  {:error
   (doall
     (map
       #(let [spec (get-in ctx [:spec (:cmd-id %)])]
          (->> %
               (m/explain spec)
               (me/humanize)))

       (:commands body)))})

(defn get-commands-response
  [ctx body]
  (dal/log-request ctx)
  (if (validate-spec ctx body)
    (let [dps-context (resolve-dependencies-to-context ctx body)
          ready-body (prepare-commands dps-context body)
          ready-context (fetch-event-sequences-for-commands dps-context ready-body)
          resp (get-response
                 ready-context
                 ready-body)]

      (if-not (:error resp)
        (->> (dal/with-transaction
               ready-context
               #(persist-events % resp))
             (persist-sequences ready-context resp)
             (add-metadata ready-context ready-body))
        resp))
    (explain-spec ctx body)))

(defn handle-commands
  [ctx body]
  (let [resp (get-commands-response ctx body)]
    (if-not (:error resp)
      {:success    true
       :fx         (reduce
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
      resp)))
