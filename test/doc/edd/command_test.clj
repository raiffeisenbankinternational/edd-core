(ns doc.edd.command-test
  (:require [clojure.test :refer [deftest is testing]]
            [doc.edd.ctx :refer [init-ctx]]
            [edd.core :as edd]
            [edd.test.fixture.dal :as mock]
            [edd.el.cmd :as el-cmd]
            [lambda.uuid :as uuid])
  (:import (clojure.lang ExceptionInfo)))

(deftest command-registration
  (testing "# To register query you need invoke edd.core/reg-cmd function"
    (-> init-ctx
        (edd/reg-cmd :test-cmd (fn [_ctx _cmd]
                                 (comment "Here you would put handler for command"))))

    (testing "Command requires handler. If handler is nil then exception is raised"
      (is (thrown? ExceptionInfo
                   (-> init-ctx
                       (edd/reg-cmd :test-cmd nil)))))
    (testing "It is poss register command schema using :consumes key"
      (-> init-ctx
          (edd/reg-cmd :test-cmd (fn [_ctx _cmd]
                                   (comment "Here you would put handler for command"))
                       :consumes [:map
                                  [:first-name :string]])))
    (testing "Schema can also be registered using :spec key but this method is deprecated"
      (-> init-ctx
          (edd/reg-cmd :test-cmd (fn [_ctx _cmd]
                                   (comment "Here you would put handler for command"))
                       :spec [:map
                              [:first-name :string]])))))

(deftest command-handler
  (testing (str "Command handler is envisioned to be pure function returning one or more events,
            Input is context and command that needs to be processed. Context wil contain
            all needed dependencies for this command (Moore later on dependencies).
            Command handler is mutating data in the system. It is necessary to have
            implementation for backend services. Ww have in memory implementation in
            " (require 'edd.test.fixture.dal) " namespace. We will start with context that is defined
            in this namespace.")
    (let [event {:event-id :tested-cmd}
          ctx (-> mock/ctx
                  (edd/reg-cmd :test-cmd (fn [_ctx _cmd]
                                           event)))
          cmd-id (uuid/gen)
          request {:cmd-id :test-cmd
                   :id     cmd-id}]

      (comment "To get response for command ve call edd.el.cmd/handle-commands
                This function you normally don't invoke directly. It is used sometimes
                for testing. Response is summarized and only shows number of items")
      (is (= {:effects    []
              :events     1
              :identities 0
              :meta       [{:test-cmd {:id cmd-id}}]
              :success    true}
             (el-cmd/handle-commands
              ctx
              {:commands [request]})))

      (comment "To see non summarized response you can pass in context
                :no-summary before invoking handle-commands. this will return
                all data that is output of request. Other fields that are here
                will be analyzed separately.")
      (is (= {:effects    []
              :events     [{:event-id       :tested-cmd
                            :id             cmd-id
                            :event-seq      2
                            :interaction-id nil
                            :meta           {:realm :test}
                            :request-id     nil}]
              :identities []
              :meta       [{:test-cmd {:id cmd-id}}]}
             (el-cmd/handle-commands
              (assoc ctx :no-summary true)
              {:commands [request]}))))
    (testing "Same request can contain multiple commands in vector. And output
              of command can be vector of events"
      (let [event-1 {:event-id :event-1}
            event-2-1 {:event-id :event-2-1}
            event-2-2 {:event-id :event-2-2}
            ctx (-> mock/ctx
                    (edd/reg-cmd :test-cmd-1 (fn [_ctx _ctxcmd]
                                               event-1))
                    (edd/reg-cmd :test-cmd-2 (fn [_ctx _cmd]
                                               [event-2-1 event-2-2])))
            cmd-id-1 (uuid/gen)
            cmd-id-2 (uuid/gen)]

        (comment "To get response for command ve call edd.el.cmd/handle-commands
                This function you normally don't invoke directly. It is used sometimes
                for testing. Response is summarized and only shows number of items")
        (is (= {:effects    []
                :events     [{:event-id       :event-1
                              :id             cmd-id-1
                              :event-seq      1
                              :interaction-id nil
                              :meta           {:realm :test}
                              :request-id     nil}
                             {:event-id       :event-2-1
                              :id             cmd-id-2
                              :event-seq      1
                              :interaction-id nil
                              :meta           {:realm :test}
                              :request-id     nil}
                             {:event-id       :event-2-2
                              :id             cmd-id-2
                              :event-seq      2
                              :interaction-id nil
                              :meta           {:realm :test}
                              :request-id     nil}]
                :identities []
                :meta       [{:test-cmd-1 {:id cmd-id-1}}
                             {:test-cmd-2 {:id cmd-id-2}}]}
               (el-cmd/handle-commands
                (assoc ctx :no-summary true)
                {:commands [{:id     cmd-id-1
                             :cmd-id :test-cmd-1}
                            {:id     cmd-id-2
                             :cmd-id :test-cmd-2}]})))))))

(deftest effects
  (testing "Outpu"
    (let [event {:event-id :tested-cmd}
          ctx (-> mock/ctx
                  (edd/reg-cmd :test-cmd (fn [_ctx _cmd]
                                           event)))
          cmd-id (uuid/gen)
          request {:cmd-id :test-cmd
                   :id     cmd-id}]

      (comment "To get response for command ve call edd.el.cmd/handle-commands
                This function you normally don't invoke directly. It is used sometimes
                for testing. Response is summarized and only shows number of items")
      (is (= {:effects    []
              :events     1
              :identities 0
              :meta       [{:test-cmd {:id cmd-id}}]
              :success    true}
             (el-cmd/handle-commands
              ctx
              {:commands [request]})))

      (comment "To see non summarized response you can pass in context
                :no-summary before invoking handle-commands. this will return
                all data that is output of request. Other fields that are here
                will be analyzed separately.")
      (is (= {:effects    []
              :events     [{:event-id       :tested-cmd
                            :id             cmd-id
                            :event-seq      2
                            :interaction-id nil
                            :meta           {:realm :test}
                            :request-id     nil}]
              :identities []
              :meta       [{:test-cmd {:id cmd-id}}]}
             (el-cmd/handle-commands
              (assoc ctx :no-summary true)
              {:commands [request]}))))))

;; ============================================================================
;; Test Fixtures Documentation
;; ============================================================================

(deftest with-mock-dal-fixture
  (testing "with-mock-dal provides in-memory stores for testing commands"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :create-user
                                 (fn [_ctx cmd]
                                   {:event-id :user-created
                                    :name (get-in cmd [:attrs :name])})))
            cmd-id (uuid/gen)]
        (mock/handle-cmd ctx {:cmd-id :create-user
                              :id cmd-id
                              :attrs {:name "Alice"}})

        (is (= [{:event-id :user-created
                 :id cmd-id
                 :event-seq 1
                 :name "Alice"}]
               (mock/peek-state :event-store))))))

  (testing "with-mock-dal can be initialized with pre-existing events"
    (let [existing-id (uuid/gen)]
      (mock/with-mock-dal
        {:event-store [{:event-id :user-created
                        :id existing-id
                        :event-seq 1
                        :name "Bob"}]}
        (is (= [{:event-id :user-created
                 :id existing-id
                 :event-seq 1
                 :name "Bob"}]
               (mock/peek-state :event-store)))))))

(deftest peek-state-fixture
  (testing "peek-state retrieves store contents without modifying them"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :create-item
                                 (fn [_ctx _cmd]
                                   {:event-id :item-created})))
            cmd-id (uuid/gen)]
        (mock/handle-cmd ctx {:cmd-id :create-item :id cmd-id})

        (comment "First peek returns the event")
        (is (= [{:event-id :item-created
                 :id cmd-id
                 :event-seq 1}]
               (mock/peek-state :event-store)))

        (comment "Second peek returns the same event - data is not removed")
        (is (= [{:event-id :item-created
                 :id cmd-id
                 :event-seq 1}]
               (mock/peek-state :event-store))))))

  (testing "peek-state without arguments returns all stores"
    (mock/with-mock-dal
      (let [all-stores (mock/peek-state)]
        (is (contains? all-stores :event-store))
        (is (contains? all-stores :command-store))
        (is (contains? all-stores :aggregate-store))
        (is (contains? all-stores :identity-store))))))

(deftest verify-state-fixture
  (testing "verify-state asserts store contents match expected"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :place-order
                                 (fn [_ctx cmd]
                                   {:event-id :order-placed
                                    :amount (get-in cmd [:attrs :amount])})))
            order-id (uuid/gen)]
        (mock/handle-cmd ctx {:cmd-id :place-order
                              :id order-id
                              :attrs {:amount 100}})

        (comment "verify-state with store-key first")
        (mock/verify-state :event-store
                           [{:event-id :order-placed
                             :id order-id
                             :event-seq 1
                             :amount 100}])

        (comment "verify-state with expected first (alternate syntax)")
        (mock/verify-state [{:event-id :order-placed
                             :id order-id
                             :event-seq 1
                             :amount 100}]
                           :event-store)))))

(deftest pop-state-fixture
  (testing "pop-state retrieves raw store contents and clears the store"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :log-action
                                 (fn [_ctx _cmd]
                                   {:event-id :action-logged})))
            cmd-id (uuid/gen)]
        (mock/handle-cmd ctx {:cmd-id :log-action :id cmd-id})

        (comment "First pop returns raw event data (including :meta)")
        (let [events (mock/pop-state :event-store)]
          (is (= 1 (count events)))
          (is (= :action-logged (:event-id (first events))))
          (is (= cmd-id (:id (first events)))))

        (comment "Second pop returns empty - data was removed")
        (is (= []
               (mock/pop-state :event-store)))))))

(deftest handle-cmd-fixture
  (testing "handle-cmd executes command and returns summary by default"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :signup
                                 (fn [_ctx _cmd]
                                   {:event-id :signed-up})))
            user-id (uuid/gen)
            result (mock/handle-cmd ctx {:cmd-id :signup :id user-id})]

        (is (= {:events 1
                :effects []
                :identities 0
                :meta [{:signup {:id user-id}}]
                :success true}
               result)))))

  (testing "handle-cmd with :no-summary returns full event details"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :register
                                 (fn [_ctx _cmd]
                                   {:event-id :registered})))
            user-id (uuid/gen)
            result (mock/handle-cmd (assoc ctx :no-summary true)
                                    {:cmd-id :register :id user-id})]

        (is (= :registered
               (-> result :events first :event-id)))
        (is (= user-id
               (-> result :events first :id)))))))

(deftest apply-cmd-fixture
  (testing "apply-cmd executes command and applies events to aggregate"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :create-account
                                 (fn [_ctx cmd]
                                   {:event-id :account-created
                                    :balance (get-in cmd [:attrs :initial-balance])}))
                    (edd/reg-event :account-created
                                   (fn [agg event]
                                     (assoc agg
                                            :status :active
                                            :balance (:balance event)))))
            account-id (uuid/gen)]

        (mock/apply-cmd ctx {:cmd-id :create-account
                             :id account-id
                             :attrs {:initial-balance 1000}})

        (comment "Events are stored")
        (mock/verify-state :event-store
                           [{:event-id :account-created
                             :id account-id
                             :event-seq 1
                             :balance 1000}])

        (comment "Aggregate is materialized")
        (is (= {:id account-id
                :version 1
                :status :active
                :balance 1000}
               (first (mock/peek-state :aggregate-store))))))))

(deftest side-effects-fixture
  (testing "reg-event-fx triggers commands that are stored in command-store"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :submit-order
                                 (fn [_ctx cmd]
                                   {:event-id :order-submitted
                                    :customer-id (get-in cmd [:attrs :customer-id])}))
                    (edd/reg-event-fx :order-submitted
                                      (fn [_ctx event]
                                        {:cmd-id :send-confirmation
                                         :id (uuid/gen)
                                         :attrs {:customer-id (:customer-id event)}})))
            order-id (uuid/gen)
            customer-id (uuid/gen)]

        (mock/handle-cmd ctx {:cmd-id :submit-order
                              :id order-id
                              :attrs {:customer-id customer-id}})

        (comment "Side effect command is stored")
        (is (= 1
               (count (mock/peek-state :command-store))))
        (is (= :send-confirmation
               (-> (mock/peek-state :command-store)
                   first :commands first :cmd-id))))))

  (testing "execute-fx processes pending side effect commands"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :create-order
                                 (fn [_ctx _cmd]
                                   {:event-id :order-created}))
                    (edd/reg-event-fx :order-created
                                      (fn [_ctx _event]
                                        {:cmd-id :notify-warehouse
                                         :id (uuid/gen)}))
                    (edd/reg-cmd :notify-warehouse
                                 (fn [_ctx _cmd]
                                   {:event-id :warehouse-notified})))
            order-id (uuid/gen)]

        (mock/handle-cmd ctx {:cmd-id :create-order :id order-id})

        (comment "Before execute-fx: only order-created event")
        (is (= 1 (count (mock/peek-state :event-store))))

        (mock/execute-fx ctx)

        (comment "After execute-fx: warehouse-notified event added")
        (is (= 2 (count (mock/peek-state :event-store))))
        (is (= :warehouse-notified
               (-> (mock/peek-state :event-store) second :event-id)))))))

(deftest execute-cmd-fixture
  (testing "execute-cmd runs command and all cascading side effects"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :process-payment
                                 (fn [_ctx _cmd]
                                   {:event-id :payment-processed}))
                    (edd/reg-event :payment-processed
                                   (fn [agg _event]
                                     (assoc agg :paid true)))
                    (edd/reg-event-fx :payment-processed
                                      (fn [_ctx _event]
                                        {:cmd-id :generate-receipt
                                         :id (uuid/gen)}))
                    (edd/reg-cmd :generate-receipt
                                 (fn [_ctx _cmd]
                                   {:event-id :receipt-generated}))
                    (edd/reg-event :receipt-generated
                                   (fn [agg _event]
                                     (assoc agg :receipt-sent true))))
            payment-id (uuid/gen)]

        (mock/execute-cmd ctx {:cmd-id :process-payment :id payment-id})

        (comment "Both events are stored")
        (is (= [:payment-processed :receipt-generated]
               (mapv :event-id (mock/peek-state :event-store))))

        (comment "Both aggregates are materialized")
        (is (= 2 (count (mock/peek-state :aggregate-store))))))))

(deftest mock-dependencies-fixture
  (testing "Dependencies can be mocked using :deps in with-mock-dal"
    (mock/with-mock-dal
      {:deps [{:service :inventory-svc
               :query {:query-id :check-stock
                       :product-id #uuid "11111111-1111-1111-1111-111111111111"}
               :resp {:in-stock true
                      :quantity 50}}]}

      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :reserve-product
                                 (fn [ctx cmd]
                                   (let [{:keys [inventory]} ctx]
                                     (if (:in-stock inventory)
                                       {:event-id :product-reserved
                                        :quantity (:quantity inventory)}
                                       {:event-id :reservation-failed})))
                                 :deps [:inventory
                                        {:service :inventory-svc
                                         :query (fn [_deps cmd]
                                                  {:query-id :check-stock
                                                   :product-id (get-in cmd [:attrs :product-id])})}]))
            cmd-id (uuid/gen)]

        (mock/handle-cmd ctx {:cmd-id :reserve-product
                              :id cmd-id
                              :attrs {:product-id #uuid "11111111-1111-1111-1111-111111111111"}})

        (is (= [{:event-id :product-reserved
                 :id cmd-id
                 :event-seq 1
                 :quantity 50}]
               (mock/peek-state :event-store)))))))

(deftest query-fixture
  (testing "mock/query executes queries against the mock DAL"
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-query :find-user
                                   (fn [_ctx query]
                                     {:id (:user-id query)
                                      :name "Test User"})))
            result (mock/query ctx {:query-id :find-user
                                    :user-id #uuid "22222222-2222-2222-2222-222222222222"})]

        (is (= {:id #uuid "22222222-2222-2222-2222-222222222222"
                :name "Test User"}
               result)))))

  (testing "mock/query can read from aggregate-store"
    (mock/with-mock-dal
      {:aggregate-store [{:id #uuid "33333333-3333-3333-3333-333333333333"
                          :version 1
                          :name "Preloaded User"}]}

      (let [ctx (-> mock/ctx
                    (edd/reg-query :get-user
                                   (fn [_ctx query]
                                     (first
                                      (filter #(= (:id %) (:user-id query))
                                              (mock/peek-state :aggregate-store))))))
            result (mock/query ctx {:query-id :get-user
                                    :user-id #uuid "33333333-3333-3333-3333-333333333333"})]

        (is (= "Preloaded User" (:name result)))))))

(deftest keep-meta-fixture
  (testing "By default peek-state will remove :meta from events. This makes
            assertions cleaner since you don't need to specify realm in every test.
            If you need to verify meta content you can use :keep-meta option."
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :create-user
                                 (fn [_ctx _cmd]
                                   {:event-id :user-created})))
            user-id (uuid/gen)]
        (mock/handle-cmd ctx {:cmd-id :create-user :id user-id})

        (comment "Without :keep-meta, meta is stripped from peek-state results")
        (is (nil? (:meta (first (mock/peek-state :event-store)))))))

    (testing "When you need to check metadata content use :keep-meta true"
      (mock/with-mock-dal
        {:keep-meta true}
        (let [ctx (-> mock/ctx
                      (edd/reg-cmd :create-user
                                   (fn [_ctx _cmd]
                                     {:event-id :user-created})))
              user-id (uuid/gen)]
          (mock/handle-cmd ctx {:cmd-id :create-user :id user-id})

          (comment "With :keep-meta true, meta is preserved in results")
          (is (= {:realm :test}
                 (:meta (first (mock/peek-state :event-store)))))

          (mock/verify-state :event-store
                             [{:event-id :user-created
                               :id user-id
                               :event-seq 1
                               :meta {:realm :test}}]))))))

(deftest breadcrumbs-fixture
  (testing "When command produces side effects (via reg-event-fx), each effect
            gets assigned breadcrumbs. Breadcrumbs are used for tracing command
            chains and preventing infinite loops. Format is [parent-index child-index]."
    (mock/with-mock-dal
      (let [ctx (-> mock/ctx
                    (edd/reg-cmd :start-process
                                 (fn [_ctx _cmd]
                                   {:event-id :process-started}))
                    (edd/reg-event-fx :process-started
                                      (fn [_ctx _event]
                                        [{:cmd-id :step-one :id (uuid/gen)}
                                         {:cmd-id :step-two :id (uuid/gen)}])))
            process-id (uuid/gen)]

        (mock/handle-cmd ctx {:cmd-id :start-process :id process-id})

        (comment "Each effect in command-store has breadcrumbs assigned.
                  Effects are wrapped - each contains :commands vector and :breadcrumbs.
                  First effect gets [0 0], second gets [0 1], etc.")
        (let [effects (mock/pop-state :command-store)]
          (is (= 2 (count effects)))
          (is (= [0 0] (:breadcrumbs (first effects))))
          (is (= [0 1] (:breadcrumbs (second effects)))))))))


