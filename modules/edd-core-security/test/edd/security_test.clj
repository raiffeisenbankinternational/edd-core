(ns edd.security-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.ctx :as edd-ctx]
            [edd.security :as security]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]))

(defn- register-orders
  [ctx]
  (let [with-create
        (edd/reg-cmd ctx
                     :create-order
                     (fn [_ cmd]
                       {:event-id :order-created
                        :attrs (:attrs cmd)}))

        with-update
        (edd/reg-cmd with-create
                     :update-order
                     (fn [_ cmd]
                       {:event-id :order-updated
                        :attrs (:attrs cmd)}))

        with-delete
        (edd/reg-cmd with-update
                     :delete-order
                     (fn [_ _]
                       {:event-id :order-deleted}))

        with-created
        (edd/reg-event with-delete
                       :order-created
                       (fn [agg _]
                         (assoc agg :status :created)))]
    (edd/reg-event with-created
                   :order-updated
                   (fn [agg _]
                     (assoc agg :status :updated)))))

(defn- secured-orders
  [& register-opts]
  (apply security/register (register-orders mock/ctx) register-opts))

(defn- allow
  [role action & {:keys [condition description]}]
  (let [policy
        {:description (or description (str "allow " action))
         :principal {:role role}
         :effect :allow
         :action action}]
    (if condition
      (assoc policy :condition condition)
      policy)))

(defn- deny
  [role action & {:keys [description]}]
  {:description (or description (str "deny " action))
   :principal {:role role}
   :effect :deny
   :action action})

(defn- with-approve-order
  [ctx auth]
  (edd/reg-cmd ctx
               :approve-order
               (fn [_ _]
                 {:event-id :order-approved})
               :auth auth))

(defn- with-order-query
  [ctx & reg-opts]
  (apply edd/reg-query
         ctx
         :get-order
         (fn [_ query]
           {:id (:id query)
            :status :created})
         reg-opts))

(defn- with-system-config-query
  [ctx]
  (edd/reg-query ctx
                 :get-system-config
                 (fn [_ _]
                   {:max-amount 100})))

(def ^:private system-config-dep
  {:system-config (fn [_ _]
                    {:query-id :get-system-config})})

(defn- run-cmd
  [ctx role cmd]
  (mock/handle-cmd (assoc ctx :meta {:realm :test
                                     :user {:role role}})
                   cmd))

(defn- run-query
  [ctx role query]
  (mock/query (assoc ctx :meta {:realm :test
                                :user {:role role}})
              query))

(defn- run-query-error
  "Runs a query expected to be rejected; returns the thrown error map."
  [ctx role query]
  (try
    (run-query ctx role query)
    ::did-not-throw
    (catch Exception e
      (:error (ex-data e)))))

(defn- denied-by
  [result]
  (get-in result [:error :policy]))

(deftest test-allow-exact-action
  (mock/with-mock-dal
    (let [ctx
          (secured-orders :policies [(allow :account-manager :create-order)])

          result
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id (uuid/gen)})]

      (is
       (true?
        (:success result))))))

(deftest test-allow-wildcard-action
  (mock/with-mock-dal
    (let [ctx
          (secured-orders :policies [(allow :account-manager :create-*)])

          created
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id (uuid/gen)})

          deleted
          (run-cmd ctx :account-manager {:cmd-id :delete-order
                                         :id (uuid/gen)})]

      (is
       (true?
        (:success created)))

      (is
       (= :implicit
          (denied-by deleted))))))

(deftest test-allow-action-vector
  (mock/with-mock-dal
    (let [ctx
          (secured-orders :policies [(allow :account-manager
                                            [:create-order :delete-order])])

          created
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id (uuid/gen)})

          deleted
          (run-cmd ctx :account-manager {:cmd-id :delete-order
                                         :id (uuid/gen)})

          updated
          (run-cmd ctx :account-manager {:cmd-id :update-order
                                         :id (uuid/gen)})]

      (is
       (true?
        (:success created)))

      (is
       (true?
        (:success deleted)))

      (is
       (= :implicit
          (denied-by updated))))))

(deftest test-implicit-deny-for-unknown-role
  (mock/with-mock-dal
    (let [ctx
          (secured-orders :policies [(allow :account-manager :create-order)])

          result
          (run-cmd ctx :auditor {:cmd-id :create-order
                                 :id (uuid/gen)})]

      (is
       (= {:meta []
           :events []
           :effects []
           :identities []
           :error {:message "Forbidden"
                   :cmd-id :create-order
                   :role :auditor
                   :policy :implicit}}
          result))

      (mock/verify-state
       :event-store
       []))))

(deftest test-deny-evaluated-before-allow
  (mock/with-mock-dal
    (let [ctx
          (secured-orders :policies [(allow :account-manager :create-order)
                                     (deny :* :create-order
                                           :description "creation is frozen")])

          result
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id (uuid/gen)})]

      (is
       (= "creation is frozen"
          (denied-by result))))))

(deftest test-non-interactive-requires-explicit-policy
  (mock/with-mock-dal
    (let [denied
          (run-cmd (secured-orders)
                   :non-interactive
                   {:cmd-id :delete-order
                    :id (uuid/gen)})

          allowed
          (run-cmd (secured-orders :policies [(allow :non-interactive :*)])
                   :non-interactive
                   {:cmd-id :delete-order
                    :id (uuid/gen)})]

      (is
       (= :implicit
          (denied-by denied)))

      (is
       (true?
        (:success allowed))))))

(deftest test-throwing-condition-fails-closed
  (mock/with-mock-dal
    (let [ctx
          (secured-orders
           :policies [(allow :account-manager :create-order
                             :condition [:fn (fn [_]
                                               (throw (ex-info "boom" {})))])])

          result
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id (uuid/gen)})]

      ;; malli treats a throwing validator as non-matching, so the
      ;; policy does not apply and the decision is the implicit deny
      (is
       (= :implicit
          (denied-by result))))))

(deftest test-condition-on-aggregate-status
  (mock/with-mock-dal
    (let [id
          (uuid/gen)

          ctx
          (secured-orders
           :policies [(allow :account-manager :create-order)
                      (allow :account-manager :update-order
                             :condition [:map
                                         [:aggregate
                                          [:map
                                           [:status [:= :created]]]]])])

          before-create
          (run-cmd ctx :account-manager {:cmd-id :update-order
                                         :id id})

          _
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id id})

          after-create
          (run-cmd ctx :account-manager {:cmd-id :update-order
                                         :id id})]

      (is
       (= :implicit
          (denied-by before-create)))

      (is
       (true?
        (:success after-create))))))

(deftest test-fn-condition-over-feature-deps
  (mock/with-mock-dal
    (let [secured-with-limit
          (fn [limit]
            (security/register
             (with-system-config-query (register-orders mock/ctx))
             :policies [(allow :account-manager :create-order
                               :condition [:fn (fn [{:keys [system-config]}]
                                                 (>= (:max-amount system-config)
                                                     limit))])]
             :deps system-config-dep))

          within-limit
          (run-cmd (secured-with-limit 50)
                   :account-manager
                   {:cmd-id :create-order
                    :id (uuid/gen)})

          over-limit
          (run-cmd (secured-with-limit 500)
                   :account-manager
                   {:cmd-id :create-order
                    :id (uuid/gen)})]

      (is
       (true?
        (:success within-limit)))

      (is
       (= :implicit
          (denied-by over-limit))))))

(deftest test-query-allowed-by-policy
  (mock/with-mock-dal
    (let [ctx
          (security/register (with-order-query (register-orders mock/ctx))
                             :policies [(allow :account-manager :get-*)])

          id
          (uuid/gen)

          result
          (run-query ctx :account-manager {:query-id :get-order
                                           :id id})]

      (is
       (= {:id id
           :status :created}
          result)))))

(deftest test-query-implicit-deny
  (mock/with-mock-dal
    (let [ctx
          (security/register (with-order-query (register-orders mock/ctx))
                             :policies [(allow :account-manager :get-*)])]

      (is
       (= {:message "Forbidden"
           :query-id :get-order
           :role :auditor
           :policy :implicit}
          (run-query-error ctx :auditor {:query-id :get-order
                                         :id (uuid/gen)}))))))

(deftest test-query-allowed-in-audit-mode
  (mock/with-mock-dal
    (let [ctx
          (security/register (with-order-query (register-orders mock/ctx))
                             :mode :audit)

          id
          (uuid/gen)

          result
          (run-query ctx :auditor {:query-id :get-order
                                   :id id})]

      (is
       (= {:id id
           :status :created}
          result)))))

(deftest test-command-deps-resolve-with-queries-denied
  (mock/with-mock-dal
    (let [ctx
          (security/register (with-system-config-query
                               (register-orders mock/ctx))
                             :policies [(allow :account-manager :create-order)]
                             :deps system-config-dep)

          result
          (run-cmd ctx :account-manager {:cmd-id :create-order
                                         :id (uuid/gen)})]

      (is
       (true?
        (:success result))))))

(deftest test-inline-auth-role
  (mock/with-mock-dal
    (let [ctx
          (security/register
           (with-approve-order (register-orders mock/ctx)
             {:role :supervisor}))

          approved
          (run-cmd ctx :supervisor {:cmd-id :approve-order
                                    :id (uuid/gen)})

          denied
          (run-cmd ctx :account-manager {:cmd-id :approve-order
                                         :id (uuid/gen)})]

      (is
       (true?
        (:success approved)))

      (is
       (= :implicit
          (denied-by denied))))))

(deftest test-inline-auth-role-vector
  (mock/with-mock-dal
    (let [ctx
          (security/register
           (with-approve-order (register-orders mock/ctx)
             {:role [:supervisor :admin]}))

          as-admin
          (run-cmd ctx :admin {:cmd-id :approve-order
                               :id (uuid/gen)})

          as-supervisor
          (run-cmd ctx :supervisor {:cmd-id :approve-order
                                    :id (uuid/gen)})]

      (is
       (true?
        (:success as-admin)))

      (is
       (true?
        (:success as-supervisor))))))

(deftest test-inline-auth-fn-receives-ctx-and-cmd
  (mock/with-mock-dal
    (let [ctx
          (with-approve-order
            (with-system-config-query (register-orders mock/ctx))
            {:fn (fn [ctx cmd]
                   (<= (get-in cmd [:attrs :amount])
                       (get-in ctx [:system-config :max-amount])))})

          ctx
          (security/register ctx :deps system-config-dep)

          within-limit
          (run-cmd ctx :account-manager {:cmd-id :approve-order
                                         :id (uuid/gen)
                                         :attrs {:amount 50}})

          over-limit
          (run-cmd ctx :account-manager {:cmd-id :approve-order
                                         :id (uuid/gen)
                                         :attrs {:amount 500}})]

      (is
       (true?
        (:success within-limit)))

      (is
       (= :implicit
          (denied-by over-limit))))))

(deftest test-inline-auth-role-and-fn-combined
  (mock/with-mock-dal
    (let [ctx
          (security/register
           (with-approve-order (register-orders mock/ctx)
             {:role :supervisor
              :fn (fn [_ cmd]
                    (get-in cmd [:attrs :reviewed]))}))

          reviewed
          (run-cmd ctx :supervisor {:cmd-id :approve-order
                                    :id (uuid/gen)
                                    :attrs {:reviewed true}})

          not-reviewed
          (run-cmd ctx :supervisor {:cmd-id :approve-order
                                    :id (uuid/gen)
                                    :attrs {:reviewed false}})

          wrong-role
          (run-cmd ctx :admin {:cmd-id :approve-order
                               :id (uuid/gen)
                               :attrs {:reviewed true}})]

      (is
       (true?
        (:success reviewed)))

      (is
       (= :implicit
          (denied-by not-reviewed)))

      (is
       (= :implicit
          (denied-by wrong-role))))))

(deftest test-mixing-central-policies-and-inline-auth-fails-at-init
  (let [ctx
        (security/register
         (with-approve-order (register-orders mock/ctx)
           {:role :supervisor})
         :policies [(allow :account-manager :create-order)])

        thrown
        (try
          (edd-ctx/init-features ctx)
          ::did-not-throw
          (catch Exception e
            e))]

    (is
     (= "Central :policies and inline :auth are exclusive"
        (ex-message thrown)))

    (is
     (= [:approve-order]
        (get (ex-data thrown) :auth-registrations)))))

(deftest test-inline-auth-on-query
  (mock/with-mock-dal
    (let [ctx
          (security/register
           (with-order-query (register-orders mock/ctx)
             :auth {:role :account-manager}))

          id
          (uuid/gen)

          result
          (run-query ctx :account-manager {:query-id :get-order
                                           :id id})]

      (is
       (= {:id id
           :status :created}
          result))

      (is
       (= :implicit
          (:policy (run-query-error ctx :auditor {:query-id :get-order
                                                  :id id})))))))

(deftest test-invalid-inline-auth-fails-at-init
  (let [ctx
        (security/register
         (with-approve-order (register-orders mock/ctx)
           {:unknown :shape}))]

    (is
     (thrown-with-msg?
      Exception
      #"Invalid registrations"
      (edd-ctx/init-features ctx)))))

(deftest test-audit-mode-does-not-enforce
  (mock/with-mock-dal
    (let [ctx
          (secured-orders :mode :audit)

          result
          (run-cmd ctx :auditor {:cmd-id :create-order
                                 :id (uuid/gen)})]

      (is
       (true?
        (:success result))))))

(deftest test-policy-schema-is-closed
  (is
   (thrown-with-msg?
    Exception
    #"Invalid policy"
    (edd-ctx/init-features
     (security/register mock/ctx
                        :policies [{:principal {:role :x
                                                :department :extra}
                                    :effect :allow
                                    :action :*}]))))

  (is
   (thrown-with-msg?
    Exception
    #"Invalid policy"
    (edd-ctx/init-features
     (security/register mock/ctx
                        :policies [(assoc (allow :x :*)
                                          :unknown-key true)])))))

(deftest test-invalid-mode-rejected
  (is
   (thrown-with-msg?
    Exception
    #"Invalid security mode"
    (security/register mock/ctx
                       :mode :dry-run))))

(deftest test-aggregate-is-reserved-dep-key
  (is
   (thrown-with-msg?
    Exception
    #"Invalid security deps"
    (security/register mock/ctx
                       :deps {:aggregate (fn [_ _]
                                           {:query-id :get-something})}))))

(deftest test-init-fails-on-malformed-condition
  (let [ctx
        (secured-orders :policies [(allow :x :*
                                          :condition [:not-a-schema 42])])]

    (is
     (thrown?
      Exception
      (edd-ctx/init-features ctx)))))
