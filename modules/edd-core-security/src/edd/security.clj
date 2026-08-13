(ns edd.security
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [malli.core :as m]
            [malli.error :as me]
            [edd.core :as edd]
            [edd.el.ctx :as el-ctx]))

(set! *warn-on-reflection* true)

(def Policy
  (m/schema
   [:map {:closed true}
    [:description
     {:optional true
      :description
      (str "Human-readable policy name, echoed in the Forbidden "
           "error and audit logs. "
           "Example: \"am can update created orders\".")}
     :string]
    [:principal
     {:description
      "Who the policy applies to, e.g. {:role :account-manager}."}
     [:map {:closed true}
      [:role
       {:description
        (str "User's selected role (from [:meta :user :role]), "
             "or :* for any role.")}
       :keyword]]]
    [:effect
     {:description
      (str ":allow grants, :deny rejects. Denies are evaluated "
           "before allows and cannot be overridden by an allow.")}
     [:enum :allow :deny]]
    [:action
     {:description
      (str "Command/query ids this policy matches: exact "
           "(:create-order), wildcard (:create-* or :*), or a "
           "vector of these. Wildcards are expanded against "
           "registered ids at startup.")}
     [:or :keyword [:vector :keyword]]]
    [:condition
     {:optional true
      :description
      (str "Malli schema over the env {:aggregate <current>, "
           "<dep-key> <value>, ...}; matching means the policy "
           "applies, omitted = always. :aggregate is nil on "
           "creation commands and queries. Example: "
           "[:map [:aggregate [:map [:status [:= :created]]]]].")}
     [:vector :any]]]))

(def Auth
  (m/schema
   [:and
    [:map {:closed true}
     [:role
      {:optional true
       :description
       (str "Role(s) allowed to invoke this command/query: keyword "
            "or vector meaning any-of. Omitted = any role. "
            "Examples: :account-manager or [:supervisor :admin].")}
      [:or :keyword [:vector :keyword]]]
     [:fn
      {:optional true
       :description
       (str "(fn [ctx cmd] boolean) - ctx carries resolved deps "
            "(aggregate via edd.el.ctx/get-aggregate), cmd is the "
            "full command/query. ANDed with :role.")}
      [:fn fn?]]]
    [:fn {:description "At least one of :role or :fn must be present."}
     (fn [{auth-role :role auth-fn :fn}]
       (or (some? auth-role)
           (some? auth-fn)))]]))

(defn- validate-policies!
  [policies]
  (doseq [policy policies]
    (when-not (m/validate Policy policy)
      (throw (ex-info "Invalid policy"
                      {:policy policy
                       :explain (me/humanize (m/explain Policy policy))})))))

(defn- dep-keys
  [deps]
  (if (map? deps)
    (keys deps)
    (map first (partition 2 deps))))

(defn- action-matches?
  [action cmd-id]
  (let [pattern
        (name action)]
    (cond
      (= action :*)
      true

      (string/ends-with? pattern "*")
      (string/starts-with? (name cmd-id)
                           (subs pattern 0 (dec (count pattern))))

      :else
      (= action cmd-id))))

(defn- policy-matches-action?
  [policy cmd-id]
  (let [actions
        (get policy :action)

        actions
        (if (keyword? actions)
          [actions]
          actions)]
    (boolean (some #(action-matches? % cmd-id) actions))))

(defn- compile-policy
  [policy]
  (let [condition
        (get policy :condition)

        validator
        (if condition
          (m/validator (m/schema condition))
          (constantly true))]
    (assoc policy :validator validator)))

(defn- policies-for-role
  [policies role]
  (filterv
   (fn [policy]
     (let [policy-role
           (get-in policy [:principal :role])]
       (or (= policy-role role)
           (= policy-role :*))))
   policies))

(defn- group-by-effect
  [policies]
  {:deny (filterv #(= :deny (:effect %)) policies)
   :allow (filterv #(= :allow (:effect %)) policies)})

(defn- group-policies
  [policies cmd-id role]
  (let [matched
        (policies-for-role
         (filterv (fn [policy]
                    (policy-matches-action? policy cmd-id))
                  policies)
         role)]
    (group-by-effect matched)))

(defn- build-action-entry
  [policies cmd-id]
  (let [roles
        (distinct
         (remove #{:*}
                 (map #(get-in % [:principal :role]) policies)))]
    (reduce
     (fn [entry role]
       (assoc entry role (group-policies policies cmd-id role)))
     {::default (group-policies policies cmd-id :*)}
     roles)))

(defn- registered-action-ids
  [ctx]
  (concat (keys (get-in ctx [:edd-core :commands]))
          (keys (get-in ctx [:edd-core :queries]))))

(defn- actions-with-inline-auth
  [ctx]
  (vec
   (for [registry
         [:commands :queries]

         [action-id options]
         (get-in ctx [:edd-core registry])

         :when (contains? options :auth)]
     action-id)))

(defn- ensure-single-auth-mode!
  "Central :policies and inline :auth are exclusive modes.
  Fails init when both are present."
  [ctx policies]
  (let [inline-actions
        (actions-with-inline-auth ctx)]
    (when (and (seq policies)
               (seq inline-actions))
      (throw (ex-info "Central :policies and inline :auth are exclusive"
                      {:message "Either register all policies centrally or put :auth on each reg-cmd/reg-query, not both"
                       :auth-registrations inline-actions})))))

(defn- auth->policy
  [action-id role auth-fn]
  (let [policy
        {:description (str "auth " action-id)
         :principal {:role role}
         :effect :allow
         :action action-id}]
    (if auth-fn
      (assoc policy :condition [:fn (fn [env]
                                      (boolean (auth-fn (get env ::ctx)
                                                        (get env ::request))))])
      policy)))

(defn- inline-auth->policies
  [ctx]
  (vec
   (for [registry
         [:commands :queries]

         [action-id options]
         (get-in ctx [:edd-core registry])

         :let [{auth-role :role auth-fn :fn}
               (get options :auth)]

         :when (contains? options :auth)

         one-role
         (cond
           (nil? auth-role) [:*]
           (keyword? auth-role) [auth-role]
           :else auth-role)]
     (auth->policy action-id one-role auth-fn))))

(defn- resolve-active-policies
  [ctx policies]
  (if (seq policies)
    policies
    (inline-auth->policies ctx)))

(defn- warn-dead-policies
  [ctx policies]
  (let [action-ids
        (registered-action-ids ctx)]
    (doseq [policy policies
            :when (not-any? #(policy-matches-action? policy %) action-ids)]
      (log/warnf "Policy matches no registered command or query: %s"
                 (get policy :description (get policy :action))))))

(defn- build-index
  [ctx policies]
  (reduce
   (fn [index action-id]
     (assoc index action-id (build-action-entry policies action-id)))
   {}
   (registered-action-ids ctx)))

(defn init
  "Resolves the active policy set (central :policies or converted inline
  :auth), validates it, expands action wildcards against registered
  commands and queries, compiles conditions and builds the lookup index.
  Warns about policies matching no registered action."
  [ctx]
  (let [{:keys [policies]}
        (get-in ctx [:edd-core :security])

        _
        (ensure-single-auth-mode! ctx policies)

        active-policies
        (resolve-active-policies ctx policies)

        _
        (validate-policies! active-policies)

        _
        (warn-dead-policies ctx active-policies)

        compiled
        (mapv compile-policy active-policies)

        index
        (build-index ctx compiled)]
    (assoc-in ctx [:edd-core :security :index] index)))

(def ^:private evaluation-order
  [:deny :allow])

(defn evaluate
  "Evaluates a compiled index entry against the env.
  Denies are evaluated before allows, any hit decides.
  No hit at all is an implicit deny.
  Returns {:effect :allow|:deny :policy <description>}."
  [entry env]
  (let [hit
        (some
         (fn [group]
           (first (filter #((:validator %) env)
                          (get entry group))))
         evaluation-order)]
    (if hit
      {:effect (get hit :effect)
       :policy (get hit :description :explicit)}
      {:effect :deny
       :policy :implicit})))

(defn- authorize
  [ctx id-key action-id request]
  (let [{:keys [index mode deps]}
        (get-in ctx [:edd-core :security])

        _
        (when-not index
          (throw (ex-info "Security feature not initialized"
                          {:message "edd.ctx/init-features was not invoked"})))

        role
        (get-in ctx [:meta :user :role])

        action-entry
        (get index action-id)

        entry
        (get action-entry role
             (get action-entry ::default {}))

        env
        (merge {:aggregate (el-ctx/get-aggregate ctx)
                ::ctx ctx
                ::request request}
               (select-keys ctx (dep-keys deps)))

        {:keys [effect policy]}
        (evaluate entry env)]
    (cond
      (= effect :allow)
      nil

      (= mode :audit)
      (do
        (log/warnf "Audit mode, %s would be denied, role: %s, policy: %s"
                   [id-key action-id]
                   role
                   policy)
        nil)

      :else
      {:error {:message "Forbidden"
               id-key action-id
               :role role
               :policy policy}})))

(defn command-filter
  "Calls the chain when authorized, otherwise returns the Forbidden
  error without invoking it."
  [ctx {:keys [cmd-id] :as cmd} chain]
  (if-let [deny (authorize ctx :cmd-id cmd-id cmd)]
    deny
    (chain ctx cmd)))

(defn query-filter
  "Calls the chain when authorized, otherwise throws the Forbidden
  error without invoking it."
  [ctx {:keys [query-id] :as query} chain]
  (if-let [deny (authorize ctx :query-id (keyword query-id) query)]
    (throw (ex-info "Forbidden" deny))
    (chain ctx query)))

(defn register
  "Registers the security feature.

  Two exclusive modes, checked at init:
  - central: provide :policies here, no :auth on registrations
  - inline: no :policies, each reg-cmd/reg-query carries :auth
    ({:role kw-or-vector, :fn (fn [ctx cmd] boolean)}), compiled at init
    into allow policies for that action

  Everything not explicitly allowed is denied - including
  :non-interactive traffic (effects, service-to-service), which needs
  its own allow policy or :auth role to keep workflows running.

  :policies vector of Policy maps
  :deps     deps resolved for every command, available in condition env
  :mode     :enforce (default) or :audit (log denials, enforce nothing)"
  [ctx & {:keys [policies deps mode]
          :or {policies []
               deps {}
               mode :enforce}}]
  (when (contains? (set (dep-keys deps)) :aggregate)
    (throw (ex-info "Invalid security deps"
                    {:message ":aggregate is a reserved env key"})))
  (when-not (contains? #{:enforce :audit} mode)
    (throw (ex-info "Invalid security mode"
                    {:mode mode})))
  (let [ctx
        (assoc-in ctx [:edd-core :security] {:policies (vec policies)
                                             :deps deps
                                             :mode mode})]
    (edd/reg-feature ctx :security
                     :deps deps
                     :init init
                     :command-filter command-filter
                     :query-filter query-filter
                     :schema [:map [:auth Auth]])))
