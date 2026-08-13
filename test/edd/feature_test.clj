(ns edd.feature-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.ctx :as edd-ctx]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]))

(defn- register-dummy-cmd
  [ctx]
  (let [with-cmd
        (edd/reg-cmd ctx
                     :dummy-cmd
                     (fn [ctx cmd]
                       {:event-id :dummy-done
                        :attrs {:flag (get-in ctx [:flag :flag])}}))]
    (edd/reg-event with-cmd
                   :dummy-done
                   (fn [agg event]
                     (assoc agg :flag (get-in event [:attrs :flag]))))))

(defn- with-flag-query
  [ctx]
  (edd/reg-query ctx
                 :get-flag
                 (fn [_ _]
                   {:flag :on})))

(defn- register-dummy-feature
  [ctx]
  (edd/reg-feature (with-flag-query ctx)
                   :dummy
                   :deps {:flag (fn [_ _]
                                  {:query-id :get-flag})}
                   :init (fn [ctx]
                           (assoc-in ctx [:dummy :ready] true))
                   :command-filter (fn [ctx cmd chain]
                                     (cond
                                       (not (get-in ctx [:dummy :ready]))
                                       {:error {:message "Init did not run"}}

                                       (get-in cmd [:attrs :blocked])
                                       {:error {:message "Blocked"
                                                :source :dummy}}

                                       :else
                                       (chain ctx cmd)))))

(deftest test-reg-feature-validation
  (is
   (thrown-with-msg?
    Exception
    #"Invalid feature registration"
    (edd/reg-feature {} :broken
                     :init :not-a-fn)))

  (is
   (thrown-with-msg?
    Exception
    #"Invalid feature registration"
    (edd/reg-feature {} :broken
                     :unknown-option true))))

(deftest test-duplicate-feature-registration-fails
  (let [ctx
        (edd/reg-feature {} :dummy)]

    (is
     (thrown-with-msg?
      Exception
      #"Feature already registered"
      (edd/reg-feature ctx :dummy)))))

(deftest test-deps-conflicts-fail-at-init-with-full-list
  (let [flag-dep
        {:flag (fn [_ _]
                 {:query-id :get-flag})}

        ctx
        (edd/reg-cmd {} :some-cmd (fn [_ _] nil)
                     :deps flag-dep)

        ctx
        (edd/reg-query ctx :some-query (fn [_ _] nil)
                       :deps flag-dep)

        ctx
        (edd/reg-feature ctx :first
                         :deps flag-dep)

        ctx
        (edd/reg-feature ctx :second
                         :deps flag-dep)

        thrown
        (try
          (edd-ctx/init-features ctx)
          nil
          (catch Exception e
            e))]

    (is
     (= "Feature deps conflicts"
        (ex-message thrown)))

    (is
     (= [{:feature :second
          :conflicts-with {:feature :first}
          :conflicting-keys [:flag]}
         {:feature :first
          :conflicts-with {:commands :some-cmd}
          :conflicting-keys [:flag]}
         {:feature :first
          :conflicts-with {:queries :some-query}
          :conflicting-keys [:flag]}
         {:feature :second
          :conflicts-with {:commands :some-cmd}
          :conflicting-keys [:flag]}
         {:feature :second
          :conflicts-with {:queries :some-query}
          :conflicting-keys [:flag]}]
        (:conflicts (ex-data thrown))))))

(deftest test-commands-sharing-deps-key-do-not-conflict
  (let [flag-dep
        {:flag (fn [_ _]
                 {:query-id :get-flag})}

        ctx
        (edd/reg-cmd {} :cmd-one (fn [_ _] nil)
                     :deps flag-dep)

        ctx
        (edd/reg-cmd ctx :cmd-two (fn [_ _] nil)
                     :deps flag-dep)

        ctx
        (edd/reg-feature ctx :dummy)]

    (is
     (true?
      (get-in (edd-ctx/init-features ctx)
              [:edd-core :features-initialized])))))

(deftest test-init-features-is-idempotent
  (let [invocations
        (atom 0)

        ctx
        (edd/reg-feature {} :counting
                         :init (fn [ctx]
                                 (swap! invocations inc)
                                 ctx))

        ctx
        (edd-ctx/init-features ctx)

        _
        (edd-ctx/init-features ctx)]

    (is
     (= 1
        @invocations))))

(deftest test-feature-schema-validated-at-init
  (let [feature
        (fn [ctx]
          (edd/reg-feature ctx :dummy
                           :schema [:map [:auth [:map {:closed true}
                                                 [:role :keyword]]]]))

        valid
        (feature
         (edd/reg-cmd {} :some-cmd (fn [_ _] nil)
                      :auth {:role :admin}))

        invalid
        (feature
         (edd/reg-cmd {} :some-cmd (fn [_ _] nil)
                      :auth {:role :admin
                             :unknown :key}))]

    (is
     (true?
      (get-in (edd-ctx/init-features valid)
              [:edd-core :features-initialized])))

    (is
     (thrown-with-msg?
      Exception
      #"Invalid registrations"
      (edd-ctx/init-features invalid)))))

(deftest test-unknown-registration-keys-fail-at-init
  (let [ctx
        (edd/reg-cmd {} :typo-cmd (fn [_ _] nil)
                     :consume [:map])

        ctx
        (edd/reg-query ctx :typo-query (fn [_ _] nil)
                       :prodces [:map])

        thrown
        (try
          (edd-ctx/init-features ctx)
          nil
          (catch Exception e
            e))]

    (is
     (= "Invalid registrations"
        (ex-message thrown)))

    (is
     (= [{:commands :typo-cmd
          :explain {:consume ["disallowed key"]}}
         {:queries :typo-query
          :explain {:prodces ["disallowed key"]}}]
        (:errors (ex-data thrown))))))

(deftest test-feature-schema-overlaps-fail-at-init-with-full-list
  (let [ctx
        (edd/reg-feature {} :first
                         :schema [:map
                                  [:consumes :any]
                                  [:auth :any]])

        ctx
        (edd/reg-feature ctx :second
                         :schema [:map [:auth :any]])

        thrown
        (try
          (edd-ctx/init-features ctx)
          nil
          (catch Exception e
            e))]

    (is
     (= "Feature schema overlaps"
        (ex-message thrown)))

    (is
     (= [{:feature :first
          :conflicts-with :edd-core
          :conflicting-keys [:consumes]}
         {:feature :second
          :conflicts-with {:feature :first}
          :conflicting-keys [:auth]}]
        (:overlaps (ex-data thrown))))))

(deftest test-feature-init-deps-and-chain-pass-through
  (mock/with-mock-dal
    (let [id
          (uuid/gen)

          ctx
          (register-dummy-feature (register-dummy-cmd mock/ctx))

          result
          (mock/handle-cmd ctx
                           {:commands [{:cmd-id :dummy-cmd
                                        :id id}]})]

      (is
       (= {:success true
           :effects []
           :events 1
           :meta [{:dummy-cmd {:id id}}]
           :identities 0}
          result))

      (mock/verify-state
       :event-store
       [{:event-id :dummy-done
         :id id
         :event-seq 1
         :attrs {:flag :on}}]))))

(deftest test-command-filter-short-circuit-stores-nothing
  (mock/with-mock-dal
    (let [id
          (uuid/gen)

          ctx
          (register-dummy-feature (register-dummy-cmd mock/ctx))

          result
          (mock/handle-cmd ctx
                           {:commands [{:cmd-id :dummy-cmd
                                        :id id
                                        :attrs {:blocked true}}]})]

      (is
       (= {:meta []
           :events []
           :effects []
           :identities []
           :error {:message "Blocked"
                   :source :dummy}}
          result))

      (mock/verify-state
       :event-store
       []))))

(deftest test-query-filter-can-reject-top-level-query
  (mock/with-mock-dal
    (let [ctx
          (edd/reg-feature (with-flag-query mock/ctx) :dummy
                           :query-filter (fn [ctx query chain]
                                           (if (= (:query-id query) :get-flag)
                                             (throw (ex-info "Blocked query" {}))
                                             (chain ctx query))))]

      (is
       (thrown-with-msg?
        Exception
        #"Blocked query"
        (mock/query ctx {:query-id :get-flag}))))))

(deftest test-dependency-queries-bypass-filters
  (mock/with-mock-dal
    (let [id
          (uuid/gen)

          ctx
          (register-dummy-cmd mock/ctx)

          ctx
          (edd/reg-feature (with-flag-query ctx) :dummy
                           :deps {:flag (fn [_ _]
                                          {:query-id :get-flag})}
                           :query-filter (fn [_ _ _]
                                           (throw (ex-info "All queries blocked" {}))))

          result
          (mock/handle-cmd ctx
                           {:commands [{:cmd-id :dummy-cmd
                                        :id id}]})]

      (is
       (true?
        (:success result)))

      (mock/verify-state
       :event-store
       [{:event-id :dummy-done
         :id id
         :event-seq 1
         :attrs {:flag :on}}]))))

(deftest test-filters-run-in-registration-order
  (mock/with-mock-dal
    (let [id
          (uuid/gen)

          ctx
          (register-dummy-cmd mock/ctx)

          ctx
          (edd/reg-feature ctx :first
                           :command-filter (fn [_ _ _]
                                             {:error {:source :first}}))

          ctx
          (edd/reg-feature ctx :second
                           :command-filter (fn [_ _ _]
                                             {:error {:source :second}}))

          result
          (mock/handle-cmd ctx
                           {:commands [{:cmd-id :dummy-cmd
                                        :id id}]})]

      (is
       (= {:source :first}
          (:error result))))))

(deftest test-effect-filter-can-drop-effects
  (mock/with-mock-dal
    (let [id
          (uuid/gen)

          ctx
          (edd/reg-event-fx (register-dummy-cmd mock/ctx)
                            :dummy-done
                            (fn [_ event]
                              {:cmd-id :follow-up
                               :id (:id event)}))

          ctx
          (edd/reg-feature ctx :dummy
                           :effect-filter (fn [ctx events chain]
                                            (remove
                                             (fn [effect]
                                               (some #(= (:cmd-id %) :follow-up)
                                                     (:commands effect)))
                                             (chain ctx events))))

          result
          (mock/handle-cmd ctx
                           {:commands [{:cmd-id :dummy-cmd
                                        :id id}]})]

      (is
       (= []
          (:effects result)))

      (mock/verify-state
       :command-store
       []))))

(deftest test-apply-without-filter-materializes
  (mock/with-mock-dal
    {:event-store [{:event-id :dummy-done
                    :id #uuid "00000000-0000-0000-0000-000000000001"
                    :event-seq 1
                    :attrs {:flag :on}}]}

    (let [id
          #uuid "00000000-0000-0000-0000-000000000001"

          ctx
          (register-dummy-cmd mock/ctx)

          _
          (mock/apply-events ctx id)]

      (mock/verify-state
       :aggregate-store
       [{:id id
         :version 1
         :flag :on}]))))

(deftest test-apply-filter-veto
  (mock/with-mock-dal
    {:event-store [{:event-id :dummy-done
                    :id #uuid "00000000-0000-0000-0000-000000000002"
                    :event-seq 1
                    :attrs {:flag :on}}]}

    (let [id
          #uuid "00000000-0000-0000-0000-000000000002"

          ctx
          (edd/reg-feature (register-dummy-cmd mock/ctx)
                           :dummy
                           :apply-filter (fn [_ _ _]
                                           {:apply false}))

          applied
          (mock/apply-events ctx id)]

      (is
       (= {:apply false}
          applied))

      (mock/verify-state
       :aggregate-store
       []))))
