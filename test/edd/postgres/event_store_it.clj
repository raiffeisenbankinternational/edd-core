(ns edd.postgres.event-store-it
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as event-store]
            [lambda.request :as request]
            [lambda.test.fixture.client :refer [verify-traffic-edn]]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.state :refer [*dal-state*]]
            [edd.core :as edd]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [lambda.test.fixture.core :as core-mock]
            [lambda.util :as util]
            [edd.common :as common]
            [clojure.tools.logging :as log]
            [lambda.core :as core]))

(def fx-id (uuid/gen))

(defn- with-realm
  [ctx]
  (assoc-in ctx [:meta :realm] :test))

(defn clean-ctx
  []
  (-> {}
      (assoc :service-name "local-test")
      (assoc :response-cache :default)
      (assoc :environment-name-lower (util/get-env "EnvironmentNameLower"))
      (assoc :aws {:region                (util/get-env "AWS_DEFAULT_REGION")
                   :account-id            (util/get-env "AccountId")
                   :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                   :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                   :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")})
      (event-store/register)
      (view-store/register)))

(defn get-ctx
  ([] (-> (clean-ctx)
          (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                [{:identity (:id cmd)}
                                 {:sequence (:id cmd)}
                                 {:id       (:id cmd)
                                  :event-id :event-1
                                  :name     (:name cmd)}
                                 {:id       (:id cmd)
                                  :event-id :event-2
                                  :name     (:name cmd)}])
                       :consumes [:map
                                  [:id uuid?]])
          (edd/reg-cmd :vmd-2 (fn [_ctx cmd]
                                [{:id       (:id cmd)
                                  :event-id :fx-event-1
                                  :name     (:name cmd)}]))
          (edd/reg-event :event-1
                         (fn [agg event]
                           (merge agg
                                  {:value "1"})))
          (edd/reg-fx (fn [ctx events]
                        [{:commands [{:cmd-id :vmd-2
                                      :id     fx-id}]
                          :service  :s2}
                         {:commands [{:cmd-id :vmd-2
                                      :id     fx-id}]
                          :service  :s2}]))
          (edd/reg-event :event-2
                         (fn [agg event]
                           (merge agg
                                  {:value "2"})))))
  ([invocation-id] (-> (get-ctx)
                       (assoc :invocation-id invocation-id))))

(deftest apply-when-two-events
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (get-ctx invocation-id)
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          req {:request-id     request-id
               :interaction-id interaction-id
               :meta           {:realm :test}
               :commands       [{:cmd-id :cmd-1
                                 :id     agg-id}]}
          apply {:request-id     interaction-id
                 :interaction-id request-id
                 :meta           {:realm :test}
                 :apply          {:aggregate-id agg-id}}
          resp (edd/handler ctx
                            (assoc req :log-level :debug))]
      (edd/handler ctx apply)
      (mock/verify-state :aggregate-store
                         [{:id      agg-id
                           :version 2
                           :value   "2"}])
      (is (= {:result         {:effects    [{:cmd-id       :vmd-2
                                             :id           fx-id
                                             :service-name :s2}
                                            {:cmd-id       :vmd-2
                                             :id           fx-id
                                             :service-name :s2}]
                               :events     2
                               :identities 1
                               :meta       [{:cmd-1 {:id agg-id}}]
                               :sequences  1
                               :success    true}
              :invocation-id  invocation-id
              :interaction-id interaction-id
              :request-id     request-id}
             resp)))))

(defn make-request
  [ctx cmds & [{:keys [invocation-id
                       request-id
                       interaction-id]
                :or {invocation-id (uuid/gen)
                     request-id (uuid/gen)
                     interaction-id (uuid/gen)}}]]
  (let [ctx (assoc ctx :invocation-id invocation-id)
        req {:request-id     request-id
             :interaction-id interaction-id
             :meta           {:realm :test}
             :commands       cmds}
        resp (edd/handler (assoc ctx :no-summary true)
                          (assoc req :log-level :debug))]
    {:invocation-id invocation-id
     :request-id request-id
     :interaction-id interaction-id
     :response resp}))

(deftest simple-command-handler-test
  (let [agg-id (uuid/gen)
        ctx (->
             (clean-ctx)
             (edd/reg-cmd :simple-cmd-1 (fn [_ctx {:keys [name]}]
                                          {:event-id :simple-event-1
                                           :name name})))]
    (edd/with-stores
      ctx
      (fn [ctx]
        (make-request ctx
                      [{:cmd-id :simple-cmd-1
                        :name "Johnny"
                        :id agg-id}])
        (mock/verify-state :aggregate-store
                           [])
        (make-request ctx
                      [{:cmd-id :simple-cmd-1
                        :name "Johnny"
                        :id agg-id}])
        (mock/verify-state :aggregate-store
                           [])
        (binding [request/*request* (atom {})]
          (edd/handler ctx
                       {:request-id     (uuid/gen)
                        :interaction-id (uuid/gen)
                        :meta           {:realm :test}
                        :apply          {:aggregate-id agg-id}}))
        (mock/verify-state :aggregate-store
                           [{:id agg-id
                             :version 2}])))))

(deftest test-request-log
  (let [ctx (get-ctx)
        ctx (-> ctx
                (edd/reg-cmd :schema-cmd (fn [_ctx _cmd]
                                           {})
                             :consumes [:map
                                        [:first-name [:string]]])
                (edd/reg-cmd :error-cmd (fn [_ctx _cmd]
                                          {:error "Dont want to handle this"}))
                (edd/reg-cmd :error-cmd (fn [_ctx _cmd]
                                          {:error "Dont want to handle this"}))
                (edd/reg-cmd :fx-cmd-1 (fn [_ctx _cmd]
                                         {:event-id :fx-event-1}))
                (edd/reg-event-fx :fx-event-1 (fn [_ctx _event]
                                                {:service-name :fx-service
                                                 :commands []}))
                (edd/reg-cmd :mix-error-cmd (fn [_ctx _cmd]
                                              [{:event-id :som-event}
                                               {:error "Dont want to handle this"}
                                               {:event-is :some-other-event}]))
                (edd/reg-cmd :exception-cmd (fn [_ctx _cmd]
                                              (throw (ex-info "Exception"
                                                              {:info "Dont know how to handle this"})))))]
    (binding [*dal-state* (atom {})]

      (testing "We should have first breadcrumnt stored as 0"
        (let [agg-id (uuid/gen)
              cmd  {:cmd-id :cmd-1
                    :id     agg-id}
              {:keys [invocation-id
                      interaction-id
                      request-id]} (make-request
                                    ctx
                                    [cmd])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (let [fx-cmd  {:meta {:realm :test},
                             :commands
                             [{:id fx-id,
                               :cmd-id :vmd-2}],
                             :request-id request-id,
                             :interaction-id interaction-id,
                             :breadcrumbs [0 0]}
                    cmd-store [{:data
                                (assoc fx-cmd
                                       :service :s2)
                                :breadcrumbs "0"}
                               {:data
                                {:service :s2,
                                 :meta {:realm :test},
                                 :commands
                                 [{:id fx-id,
                                   :cmd-id :vmd-2}],
                                 :request-id request-id,
                                 :interaction-id interaction-id,
                                 :breadcrumbs [0 1]},
                                :breadcrumbs "0"}]]
                (is (= {:request-id request-id
                        :interaction-id interaction-id
                        :commands [cmd]
                        :breadcrumbs [0]
                        :log-level :debug,
                        :meta {:realm :test}}
                       (-> (event-store/get-request-log (with-realm ctx)
                                                        {:invocation-id invocation-id})
                           first
                           :data)))
                (is (= "0"
                       (-> (event-store/get-request-log (with-realm ctx)
                                                        {:invocation-id invocation-id})
                           first
                           :breadcrumbs)))
                (is (= "0"
                       (-> (event-store/get-request-log (with-realm ctx)
                                                        {:request-id request-id})
                           first
                           :breadcrumbs)))
                (is (= "0"
                       (-> (event-store/get-request-log (with-realm ctx)
                                                        {:interaction-id interaction-id})
                           first
                           :breadcrumbs)))

                (is (= "0"
                       (-> (event-store/get-response-log (with-realm ctx)
                                                         {:invocation-id invocation-id})
                           first
                           :breadcrumbs)))
                (is (= "0"
                       (-> (event-store/get-response-log (with-realm ctx)
                                                         {:request-id request-id})
                           first
                           :breadcrumbs)))
                (is (= "0"
                       (-> (event-store/get-response-log (with-realm ctx)
                                                         {:interaction-id interaction-id})
                           first
                           :breadcrumbs)))

                (is (= cmd-store
                       (->> (event-store/get-command-store (with-realm ctx)
                                                           {:invocation-id invocation-id})
                            (mapv #(select-keys % [:data :breadcrumbs])))))
                (is (= cmd-store
                       (->> (event-store/get-command-store (with-realm ctx)
                                                           {:request-id request-id})
                            (mapv #(select-keys % [:data :breadcrumbs])))))
                (is (= cmd-store
                       (->> (event-store/get-command-store (with-realm ctx)
                                                           {:request-id request-id
                                                            :breadcrumbs [0]})
                            (mapv #(select-keys % [:data :breadcrumbs])))))
                (is (= cmd-store
                       (->> (event-store/get-command-store (with-realm ctx)
                                                           {:interaction-id interaction-id
                                                            :breadcrumbs [0]})
                            (mapv #(select-keys % [:data :breadcrumbs])))))
                (testing "Wre execute fx-command. We expect that 
                          we will not store full request in request-log 
                          because it is already stored in command-store"
                  (let [invocation-id-fx (uuid/gen)]
                    (edd/handler (assoc ctx
                                        :invocation-id invocation-id-fx)
                                 fx-cmd)
                    (is (= "0:0"
                           (-> (event-store/get-request-log (with-realm ctx)
                                                            {:invocation-id invocation-id-fx})
                               first
                               :breadcrumbs)))
                    (is (= "0:0"
                           (-> (event-store/get-request-log (with-realm ctx)
                                                            {:request-id request-id
                                                             :breadcrumbs [0 0]})
                               first
                               :breadcrumbs)))
                    (is (= ["0:0"]
                           (-> (map :breadcrumbs
                                    (event-store/get-request-log (with-realm ctx)
                                                                 {:interaction-id interaction-id
                                                                  :breadcrumbs [0 0]})))))
                    (is (= {:ref [0]}
                           (-> (event-store/get-request-log (with-realm ctx)
                                                            {:request-id request-id
                                                             :breadcrumbs [0 0]})
                               first
                               :data))))))))))

      (testing "When FX commands is empty list"
        (let [agg-id (uuid/gen)
              {:keys [invocation-id]} (make-request
                                       ctx
                                       [{:cmd-id :fx-cmd-1
                                         :id     agg-id}])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (let [r (event-store/get-request-log (with-realm ctx)
                                                   {:invocation-id invocation-id})]
                (is (= nil
                       (-> r
                           first
                           :error))))
              (let [r (event-store/get-response-log (with-realm ctx)
                                                    {:invocation-id invocation-id})]
                (is (= nil
                       (-> r
                           first
                           :data
                           :error))))))))

      (testing "When we have schema validation 
               or command handler error we should see 
               in command-response-log"
        (let [agg-id (uuid/gen)
              {:keys [invocation-id]} (make-request
                                       ctx
                                       [{:cmd-id :schema-cmd
                                         :id     agg-id
                                         :first-name :invalit-type}])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (let [r (event-store/get-request-log (with-realm ctx)
                                                   {:invocation-id invocation-id})]
                (is (= nil
                       (-> r
                           first
                           :error))))
              (let [r (event-store/get-response-log (with-realm ctx)
                                                    {:invocation-id invocation-id})]
                (is (= {:first-name ["should be a string"]}
                       (-> r
                           first
                           :data
                           :error))))))))

      (testing "Test command returningn error"
        (let [agg-id (uuid/gen)
              {:keys [invocation-id]} (make-request
                                       ctx
                                       [{:cmd-id :error-cmd
                                         :id     agg-id
                                         :first-name :invalit-type}])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (is (= nil
                     (-> (event-store/get-request-log (with-realm ctx)
                                                      {:invocation-id invocation-id})
                         first
                         :error)))
              (is (= [{:id     agg-id
                       :error "Dont want to handle this"}]
                     (-> (event-store/get-response-log (with-realm ctx)
                                                       {:invocation-id invocation-id})
                         first
                         :data
                         :error)))))))

      (testing "Test command returningn mix of error and events"
        (let [agg-id (uuid/gen)
              {:keys [invocation-id
                      resp]} (make-request
                              ctx
                              [{:cmd-id :mix-error-cmd
                                :id     agg-id
                                :first-name :invalit-type}])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (is (= nil
                     (-> (event-store/get-request-log (with-realm ctx)
                                                      {:invocation-id invocation-id})
                         first
                         :error)))
              (is (= [{:id     agg-id
                       :error "Dont want to handle this"}]
                     (-> (event-store/get-response-log (with-realm ctx)
                                                       {:invocation-id invocation-id})
                         first
                         :data
                         :error)))))))

      (testing "Test multiple commands where one of them returns error"
        (let [agg-id (uuid/gen)
              {:keys [invocation-id]} (make-request
                                       ctx
                                       [{:cmd-id :cmd-1
                                         :id     agg-id}
                                        {:cmd-id :mix-error-cmd
                                         :id     agg-id
                                         :first-name :invalit-type}
                                        {:cmd-id :cmd-1
                                         :id     agg-id}])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (is (= nil
                     (-> (event-store/get-request-log (with-realm ctx)
                                                      {:invocation-id invocation-id})
                         first
                         :error)))
              (is (= [{:id     agg-id
                       :error "Dont want to handle this"}]
                     (-> (event-store/get-response-log (with-realm ctx)
                                                       {:invocation-id invocation-id})
                         first
                         :data
                         :error)))))))
      (testing "Test command raising exception"
        (let [agg-id (uuid/gen)
              {:keys [invocation-id]} (make-request
                                       ctx
                                       [{:cmd-id :exception-cmd
                                         :id     agg-id
                                         :first-name :invalit-type}])]
          (edd/with-stores
            ctx
            (fn [ctx]
              (is (= {:info "Dont know how to handle this"}
                     (-> (event-store/get-request-log (with-realm ctx)
                                                      {:invocation-id invocation-id})
                         first
                         :error)))
              (is (= nil
                     (-> (event-store/get-response-log (with-realm ctx)
                                                       {:invocation-id invocation-id})
                         first
                         :data
                         :error))))))))))

(deftest test-transaction-when-saving
  (with-redefs [event-store/store-effects (fn [_ctx  _resp]
                                            (throw (ex-info "CMD Store failure" {})))]
    (binding [*dal-state* (atom {})]
      (let [invocation-id (uuid/gen)
            ctx (get-ctx invocation-id)
            agg-id (uuid/gen)
            interaction-id (uuid/gen)
            request-id (uuid/gen)
            req {:request-id     request-id
                 :interaction-id interaction-id
                 :meta           {:realm :test}
                 :commands       [{:cmd-id :cmd-1
                                   :id     agg-id}]}
            resp (edd/handler (assoc ctx :no-summary true)
                              (assoc req :log-level :debug))]

        (edd/with-stores
          ctx (fn [ctx]
                (is (= []
                       (event-store/get-response-log
                        (with-realm ctx)
                        {:invocation-id invocation-id})))))

        (is (= {:exception      "CMD Store failure"
                :invocation-id  invocation-id
                :interaction-id interaction-id
                :request-id     request-id}
               resp))))))

(deftest test-sequence-when-saving-error
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (-> (get-ctx invocation-id)
                  (edd/reg-cmd :cmd-i1 (fn [ctx cmd]
                                         [{:sequence (:id cmd)}
                                          {:event-id :i1-created}]))
                  (edd/reg-cmd :cmd-i2 (fn [ctx cmd]
                                         [{:event-id :i1-created}])
                               :deps {:seq edd.common/get-sequence-number-for-id})
                  (edd/reg-fx (fn [ctx events]
                                {:commands [{:cmd-id :cmd-i2
                                             :id     (:id (first events))}]
                                 :service  :s2})))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          agg-id-2 (uuid/gen)
          req {:request-id     request-id
               :interaction-id interaction-id
               :meta           {:realm :test}
               :commands       [{:cmd-id :cmd-i1
                                 :id     agg-id}]}
          resp (with-redefs [event-store/update-sequence (fn [ctx cmd]
                                                           (throw (ex-info "Sequence Store failure" {})))]
                 (edd/handler ctx
                              (assoc req :log-level :debug)))]

      (edd/handler ctx
                   {:request-id     (uuid/gen)
                    :interaction-id interaction-id
                    :meta           {:realm :test}
                    :commands       [{:cmd-id :cmd-i1
                                      :id     agg-id-2}]})
      (edd/with-stores
        ctx (fn [ctx]
              (is (not= []
                        (event-store/get-response-log
                         (with-realm ctx)
                         {:invocation-id invocation-id})))
              (let [seq (event-store/get-sequence-number-for-id-imp
                         (with-realm (assoc ctx :id agg-id)))]
                (is (> seq
                       0))
                (is (= (dec seq)
                       (event-store/get-sequence-number-for-id-imp
                        (with-realm (assoc ctx :id agg-id-2))))))
              (is (= nil
                     (event-store/get-sequence-number-for-id-imp
                      (with-realm (assoc ctx :id (uuid/gen))))))))

      (is (= {:exception      "Sequence Store failure"
              :invocation-id  invocation-id
              :interaction-id interaction-id
              :request-id     request-id}
             resp)))))

(deftest test-saving-of-request-error
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (get-ctx invocation-id)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          req {:request-id     request-id
               :interaction-id interaction-id
               :meta           {:realm :test}
               :commands       [{:cmd-id :cmd-1
                                 :no-id  "schema should fail"}]}
          resp (edd/handler ctx
                            (assoc req :log-level :debug))
          expected-error {:id ["missing required key"]}]

      (edd/with-stores
        ctx (fn [ctx]
              (is (= [expected-error]
                     (mapv #(get-in % [:data :error])
                           (event-store/get-response-log (with-realm ctx)
                                                         {:request-id request-id
                                                          :breadcrumbs [0]}))))))

      (is (= {:error      expected-error
              :invocation-id  invocation-id
              :interaction-id interaction-id
              :request-id     request-id}
             resp)))))

(deftest test-event-store-multiple-commands
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (-> (get-ctx invocation-id)
                  (with-realm))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)]

      (edd/handler ctx
                   {:request-id     request-id
                    :interaction-id interaction-id
                    :meta           {:realm :test}
                    :commands       [{:cmd-id :cmd-1
                                      :id     agg-id}]})
      (edd/handler ctx
                   {:request-id     interaction-id
                    :interaction-id request-id
                    :meta           {:realm :test}
                    :apply          {:aggregate-id agg-id}})
      (mock/verify-state :aggregate-store
                         [{:id      agg-id
                           :version 2
                           :value   "2"}])

      (event-store/verify-state ctx
                                interaction-id
                                :event-store
                                [{:event-id       :event-1
                                  :event-seq      1
                                  :id             agg-id
                                  :interaction-id interaction-id
                                  :meta           {:realm :test}
                                  :name           nil
                                  :request-id     request-id}
                                 {:event-id       :event-2
                                  :event-seq      2
                                  :id             agg-id
                                  :interaction-id interaction-id
                                  :meta           {:realm :test}
                                  :name           nil
                                  :request-id     request-id}]))))

(deftest test-duplicate-identity-store-error
  (binding [*dal-state* (atom {})]
    (let [invocation-id (uuid/gen)
          ctx (get-ctx invocation-id)
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)]

      (edd/handler ctx
                   {:request-id     request-id
                    :interaction-id interaction-id
                    :meta           {:realm :test}
                    :commands       [{:cmd-id :cmd-1
                                      :id     agg-id}]})
      (edd/handler ctx
                   {:request-id     interaction-id
                    :interaction-id request-id
                    :meta           {:realm :test}
                    :apply          {:aggregate-id agg-id}})
      (mock/verify-state :aggregate-store
                         [{:id      agg-id
                           :version 2
                           :value   "2"}])

      (let [resp (edd/handler ctx
                              {:request-id     request-id
                               :interaction-id interaction-id
                               :meta           {:realm :test}
                               :commands       [{:cmd-id  :cmd-1
                                                 :version 2
                                                 :id      agg-id}]})]
        (is = ({:exception      {:key              :concurrent-modification
                                 :original-message "ERROR: duplicate key value violates unique constraint \"part_identity_store_31_pkey\"  Detail: Key (aggregate_id, id)=(06bf5f2a-de98-4b0f-bea9-d0fb3f4be51f, 06bf5f2a-de98-4b0f-bea9-d0fb3f4be51f) already exists."}
                :interaction-id interaction-id
                :invocation-id  invocation-id
                :request-id     request-id}
               resp))))))

(deftest test-find-identity
  ; Just to play wit logger
  #_(alter-var-root #'clojure.tools.logging/*logger-factory*
                    (fn [f] (lambda-logging/slf4j-json-factory)))
  (log/error "sasa" (ex-info "da" {}))
  (binding [*dal-state* (atom {})
            lambda.request/*request* (atom {:mdc {:log-level "debug"}})]
    (let [invocation-id (uuid/gen)
          ctx (get-ctx invocation-id)
          ctx (assoc-in ctx [:meta :realm] :test)
          ctx (edd/reg-cmd ctx :cmd-id-1 (fn [ctx cmd]
                                           [{:identity (:ccn cmd)}]))
          agg-id (uuid/gen)
          agg-id-2 (uuid/gen)
          agg-id-3 (uuid/gen)
          ccn1 (str "c1-" (uuid/gen))
          ccn2 (str "c2-" (uuid/gen))
          ccn3 (str "c3-" (uuid/gen))
          interaction-id (uuid/gen)]

      (edd/handler ctx
                   {:request-id     (uuid/gen)
                    :interaction-id interaction-id
                    :meta           {:realm :test}
                    :commands       [{:cmd-id :cmd-id-1
                                      :ccn    ccn1
                                      :id     agg-id}]})
      (edd/handler ctx
                   {:request-id     (uuid/gen)
                    :interaction-id interaction-id
                    :meta           {:realm :test}
                    :commands       [{:cmd-id :cmd-id-1
                                      :ccn    ccn2
                                      :id     agg-id-2}]})

      (edd/handler ctx
                   {:request-id     (uuid/gen)
                    :interaction-id interaction-id
                    :meta           {:realm :test}
                    :commands       [{:cmd-id :cmd-id-1
                                      :ccn    ccn3
                                      :id     agg-id-3}]})

      (edd/with-stores
        ctx
        (fn [ctx]
          (is (= nil
                 (common/get-aggregate-id-by-identity ctx {:ccn nil})))
          (is (= {}
                 (common/get-aggregate-id-by-identity ctx {:ccn []})))
          (is (= {ccn1 agg-id
                  ccn2 agg-id-2}
                 (common/get-aggregate-id-by-identity ctx {:ccn [ccn1 ccn2 "777777"]})))
          (is (= agg-id-3
                 (common/get-aggregate-id-by-identity ctx {:ccn ccn3}))))))))

(deftest test-retry-with-cache
  "We need to ensure that we send retry events to sqs
   when response is cached to s3 and we retry"
  (let [aggregate-id (uuid/gen)
        attempt (atom 0)
        request-id (uuid/gen)
        interaction-id (uuid/gen)
        invocation-id-1 (uuid/gen)
        invocation-id-2 (uuid/gen)
        environment-name-lower (util/get-env "EnvironmentNameLower" "local")
        account-id (util/get-env "AccountId" "local")
        ctx (-> mock/ctx
                (event-store/register)
                (view-store/register)
                (dissoc :response-cache)
                (edd.response.s3/register)
                (with-realm)
                (assoc :aws {:aws-session-token "tok"})
                (assoc :service-name "retry-test")
                (edd/reg-cmd :error-prone (fn [ctx cmd]
                                            [{:sequence (:id cmd)}
                                             {:event-id :error-prone-event
                                              :attr     "sa"}])))]
    (with-redefs [sdk.aws.s3/put-object (fn [ctx object]
                                          {:status 200})
                  sdk.aws.common/authorize (fn [_] "")
                  sdk.aws.common/create-date (fn [] "20220304T113706Z")
                  edd.postgres.event-store/update-sequence (fn [_ctx _sequence]
                                                             (when (= @attempt 0)
                                                               (swap! attempt inc)
                                                               (throw (ex-info "Timeout"
                                                                               {:data "Timeout"}))))]
      (core-mock/mock-core
       :invocations [(util/to-json {:request-id     request-id
                                    :interaction-id interaction-id
                                    :invocation-id  invocation-id-1
                                    :commands       [{:cmd-id :error-prone
                                                      :id     aggregate-id}]})
                     (util/to-json {:request-id     request-id
                                    :interaction-id interaction-id
                                    :invocation-id  invocation-id-2
                                    :commands       [{:cmd-id :error-prone
                                                      :id     aggregate-id}]})]

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"}]
       (core/start
        ctx
        edd/handler)
       (verify-traffic-edn [{:body   {:result         {:meta       [{:error-prone {:id aggregate-id}}],
                                                       :events     1,
                                                       :sequences  1,
                                                       :success    true,
                                                       :effects    [],
                                                       :identities 0},
                                      :invocation-id  invocation-id-2
                                      :request-id     request-id
                                      :interaction-id interaction-id}
                             :method :post
                             :url    (str "http://mock/2018-06-01/runtime/invocation/"
                                          invocation-id-2
                                          "/response")}
                            {:body            (str "Action=SendMessage&MessageBody=%7B%22Records%22%3A%5B%7B%22s3%22%3A%7B%22object%22%3A%7B%22key%22%3A%22response%2F"
                                                   request-id
                                                   "%2F0%2Flocal-test.json%22%7D%2C%22bucket%22%3A%7B%22name%22%3A%22"
                                                   account-id
                                                   "-"
                                                   environment-name-lower
                                                   "-sqs%22%7D%7D%7D%5D%7D")
                             :connect-timeout 300
                             :headers         {"Accept"               "application/json"
                                               "Authorization"        ""
                                               "Content-Type"         "application/x-www-form-urlencoded"
                                               "X-Amz-Date"           "20220304T113706Z"
                                               "X-Amz-Security-Token" "tok"}
                             :idle-timeout    5000
                             :method          :post
                             :url             (str "https://sqs.eu-central-1.amazonaws.com/"
                                                   account-id
                                                   "/"
                                                   environment-name-lower
                                                   "-glms-router-svc-response")
                             :version         :http1.1}
                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}
                            {:body   {:error          "Timeout"
                                      :invocation-id  invocation-id-1
                                      :request-id     request-id
                                      :interaction-id interaction-id}
                             :method :post
                             :url    (str "http://mock/2018-06-01/runtime/invocation/"
                                          invocation-id-1
                                          "/error")}
                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(defn get-fx-meta
  [ctx req]
  (edd/with-stores
    ctx
    (fn [ctx]
      (->> (event-store/get-response-log ctx
                                         req)
           (sort-by :breadcrumbs)
           (mapv #(select-keys %
                               [:fx-error
                                :fx-exception
                                :fx-remaining
                                :fx-created
                                :fx-processed]))))))

(defn get-request-meta
  [ctx req]
  (edd/with-stores
    ctx
    (fn [ctx]
      (->> (event-store/get-request-log ctx
                                        req)
           (sort-by :breadcrumbs)
           (mapv #(select-keys %
                               [:fx-exception]))))))

(defn get-response-trace-log
  [ctx req]
  (edd/with-stores
    ctx
    (fn [ctx]
      (->> (event-store/get-response-trace-log ctx req)
           (sort-by :breadcrumbs)))))

(defn get-request-trace-log
  [ctx req]
  (edd/with-stores
    ctx
    (fn [ctx]
      (->> (event-store/get-request-trace-log ctx req)
           (sort-by :breadcrumbs)))))

(deftest test-commands-stracking
  (binding [*dal-state* (atom {})]
    (let [ctx (-> (clean-ctx)
                  (assoc :invocation-id (uuid/gen))
                  (with-realm)
                  (edd/reg-cmd :cmd-level-1 (fn [_ _]
                                              {:event-id :event-level-1}))
                  (edd/reg-cmd :cmd-level-2 (fn [_ _]
                                              {:event-id :event-level-2}))
                  (edd/reg-event-fx :event-level-1 (fn [_ event]
                                                     {:cmd-id :cmd-level-2
                                                      :id (:id event)})))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)]

      (let [resp (edd/handler (assoc ctx :no-summary true)
                              {:request-id     request-id
                               :interaction-id interaction-id
                               :meta           {:realm (get-in ctx [:meta :realm])}
                               :commands       [{:cmd-id :cmd-level-1
                                                 :id     agg-id}]})]

        (is (= [{:fx-error 0
                 :fx-created 1
                 :fx-processed 0}]
               (get-fx-meta ctx {:request-id request-id})))
        (doseq [cmd (get-in resp [:result :effects])]
          (edd/handler (assoc ctx :no-summary true)
                       cmd))
        (is (= [{:fx-created 1
                 :fx-error 0
                 :fx-processed 0}
                {:fx-created 0
                 :fx-error 0
                 :fx-processed 1}]
               (get-fx-meta ctx {:request-id request-id})))
        (is (= [{:fx-created 1
                 :fx-processed 1
                 :fx-error 0
                 :fx-remaining 0}]
               (get-response-trace-log ctx {:request-id request-id})))))))

(deftest test-commands-with-error
  (binding [*dal-state* (atom {})]
    (let [ctx (-> (clean-ctx)
                  (assoc :invocation-id (uuid/gen))
                  (with-realm)
                  (edd/reg-cmd :cmd-level-1 (fn [_ _]
                                              {:event-id :event-level-1}))
                  (edd/reg-cmd :cmd-level-2 (fn [_ _]
                                              {:error "Level 2 error"}))
                  (edd/reg-event-fx :event-level-1 (fn [_ event]
                                                     {:cmd-id :cmd-level-2
                                                      :id (:id event)})))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)]

      (let [resp (edd/handler (assoc ctx :no-summary true)
                              {:request-id     request-id
                               :interaction-id interaction-id
                               :meta           {:realm (get-in ctx [:meta :realm])}
                               :commands       [{:cmd-id :cmd-level-1
                                                 :id     agg-id}]})]

        (is (= [{:fx-error 0
                 :fx-processed 0
                 :fx-created 1}]
               (get-fx-meta ctx {:request-id request-id})))
        (doseq [cmd (get-in resp [:result :effects])]
          (edd/handler (assoc ctx :no-summary true)
                       cmd))
        (is (= [{:fx-processed 0
                 :fx-error 0
                 :fx-created 1}
                {:fx-error 1
                 :fx-created 0
                 :fx-processed 0}]
               (get-fx-meta ctx {:request-id request-id})))
        (is (= [{:fx-created 1
                 :fx-processed 0
                 :fx-error 1
                 :fx-remaining 0}]
               (get-response-trace-log ctx {:request-id request-id})))))))

(deftest test-commands-with-exception
  (binding [*dal-state* (atom {})]
    (let [attempt (atom 0)
          max-attemnt 2
          ctx (-> (clean-ctx)
                  (assoc :invocation-id (uuid/gen))
                  (with-realm)
                  (edd/reg-cmd :cmd-level-1 (fn [_ _]
                                              {:event-id :event-level-1}))
                  (edd/reg-cmd :cmd-level-2 (fn [_ _]
                                              (when (< @attempt max-attemnt)
                                                (swap! attempt inc)
                                                (throw (ex-info "Leve2 exception"
                                                                {:message "Level2 exceptiopn"})))))
                  (edd/reg-event-fx :event-level-1 (fn [_ event]
                                                     {:cmd-id :cmd-level-2
                                                      :id (:id event)})))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          request {:request-id     request-id
                   :interaction-id interaction-id
                   :meta           {:realm (get-in ctx [:meta :realm])}
                   :commands       [{:cmd-id :cmd-level-1
                                     :id     agg-id}]}]

      (let [resp (edd/handler (assoc ctx :no-summary true)
                              request)]

        (is (= [{:fx-error 0
                 :fx-processed 0
                 :fx-created 1}]
               (get-fx-meta ctx {:request-id request-id})))
        (doseq [cmd (get-in resp [:result :effects])]
          (edd/handler (assoc ctx :no-summary true)
                       cmd))
        (is (= [{:fx-error 0
                 :fx-processed 0
                 :fx-created 1}]
               (get-fx-meta ctx {:request-id request-id})))

        (is (= [{:fx-exception 0}]
               (get-request-meta ctx {:request-id request-id})))

        (doseq [cmd (get-in resp [:result :effects])]
          (edd/handler (assoc ctx :no-summary true)
                       cmd))
        (doseq [cmd (get-in resp [:result :effects])]
          (edd/handler (assoc ctx :no-summary true)
                       cmd))

        (is (= [{:fx-created 1
                 :fx-processed 1
                 :fx-error 0
                 :fx-remaining 0}]
               (get-response-trace-log ctx {:request-id request-id})))
        (testing (format "We tried %s time" max-attemnt)
          (is (= [{:fx-exception max-attemnt}]
                 (get-request-trace-log ctx {:request-id request-id}))))))))

(deftest parent-bradcrumbs
  (is (= []
         (event-store/get-parent-breadcrumbs [])))
  (is (= []
         (event-store/get-parent-breadcrumbs [0])))
  (is (= [[0]]
         (event-store/get-parent-breadcrumbs [0 1])))
  (is (= [[0 1] [0]]
         (event-store/get-parent-breadcrumbs [0 1 2]))))

(deftest test-commands-stracking-multiple
  (binding [*dal-state* (atom {})]
    (let [ctx (-> (clean-ctx)
                  (assoc :invocation-id (uuid/gen))
                  (with-realm)
                  (edd/reg-cmd :cmd-level-1 (fn [_ _]
                                              {:event-id :event-level-1}))
                  (edd/reg-cmd :cmd-level-2 (fn [_ _]
                                              {:event-id :event-level-2}))
                  (edd/reg-cmd :cmd-level-3 (fn [_ _]
                                              {:event-id :event-level-3}))
                  (edd/reg-event-fx :event-level-1 (fn [_ event]
                                                     [{:cmd-id :cmd-level-2
                                                       :id (:id event)}
                                                      {:cmd-id :cmd-level-2
                                                       :id (:id event)}]))
                  (edd/reg-event-fx :event-level-2 (fn [_ event]
                                                     {:cmd-id :cmd-level-3
                                                      :id (:id event)})))
          agg-id (uuid/gen)
          interaction-id (uuid/gen)
          request-id (uuid/gen)
          resp (edd/handler (assoc ctx :no-summary true)
                            {:request-id     request-id
                             :interaction-id interaction-id
                             :meta           {:realm (get-in ctx [:meta :realm])}
                             :commands       [{:cmd-id :cmd-level-1
                                               :id     agg-id}]})]

      (is (= [{:fx-error 0
               :fx-created 2
               :fx-processed 0}]
             (get-fx-meta ctx {:request-id request-id})))
      (let [resp (mapv
                  #(edd/handler (assoc ctx :no-summary true)
                                %)
                  (get-in resp [:result :effects]))]

        (is (= [{:fx-processed 0
                 :fx-error 0
                 :fx-created 2}
                {:fx-processed 1
                 :fx-error 0
                 :fx-created 1}
                {:fx-processed 1
                 :fx-error 0
                 :fx-created 1}]
               (get-fx-meta ctx {:request-id request-id})))
        (mapv
         #(edd/handler (assoc ctx :no-summary true)
                       %)
         (get-in (first
                  resp)
                 [:result :effects]))
        (is (= [{:fx-processed 0
                 :fx-error 0
                 :fx-created 2}
                {:fx-processed 1
                 :fx-error 0
                 :fx-created 1}
                {:fx-processed 1
                 :fx-error 0
                 :fx-created 0}
                {:fx-processed 1
                 :fx-error 0
                 :fx-created 1}]
               (get-fx-meta ctx {:request-id request-id})))
        (is (= [{:fx-created 4
                 :fx-processed 3
                 :fx-error 0
                 :fx-remaining 1}]
               (get-response-trace-log ctx {:request-id request-id})))))))

(defn incnil
  [p]
  (if p
    (inc p)
    1))

#_(deftest test-commands-stracking-random
    (binding [*dal-state* (atom {})]
      (let [fx-per-command 2
            counts (atom {:fx-total 0
                          :fx-error 0
                          :fx-exception 0
                          :fx-remaining 0})
            next-cmd (fn [event]
                       (mapv
                        (fn [_]
                          (let [next-num (+ 2
                                            (rand-int 4))
                                        ; Lest make it a bit more longer and avoid exit early
                                next-num (if (and
                                              (= next-num
                                                 5)
                                              (< (:fx-total @counts)
                                                 20))
                                           2
                                           next-num)]

                            (swap! counts #(-> %
                                               (update :fx-total inc)
                                               (update :fx-remaining inc)))
                            {:service "local-test"
                             :commands [{:cmd-id (-> (str "cmd-level-" next-num)
                                                     keyword)
                                         :id (:id event)}]}))
                        (range (if (> (:fx-total @counts)
                                      100)
                                 0
                                 fx-per-command))))

            ctx (-> (clean-ctx)
                    (assoc :invocation-id (uuid/gen))
                    (with-realm)
                    (edd/reg-cmd :cmd-level-1 (fn [_ _]
                                                {:event-id :event-level-1}))
                    (edd/reg-cmd :cmd-level-2 (fn [_ _]
                                                (swap! counts #(-> %
                                                                   #_(update :fx-remaining dec)
                                                                   (update :cmd-level-2 incnil)))
                                                {:event-id :event-level-2}))
                    (edd/reg-cmd :cmd-level-3 (fn [_ _]
                                                (swap! counts #(-> %
                                                                   #_(update :fx-error inc)
                                                                   #_(update :fx-remaining dec)
                                                                   (update :cmd-level-3 incnil)))
                                                {:error "Level 3 error"}))
                    (edd/reg-cmd :cmd-level-4 (fn [_ _]
                                                (swap! counts #(-> %
                                                                   #_(update :fx-remaining dec)
                                                                   (update :cmd-level-4 incnil)))
                                                {:event-id :event-level-4}))
                    (edd/reg-cmd :cmd-level-5 (fn [_ _]
                                                (swap! counts #(-> %
                                                                   #_(update :fx-remaining dec)
                                                                   (update :cmd-level-5 incnil)))
                                                {:event-id :event-level-5}))
                    (edd/reg-event-fx :event-level-1 (fn [_ event]
                                                       (next-cmd event)))
                    (edd/reg-event-fx :event-level-2 (fn [_ event]
                                                       (next-cmd event)))
                    (edd/reg-event-fx :event-level-4 (fn [_ event]
                                                       (next-cmd event))))
            agg-id (uuid/gen)
            interaction-id (uuid/gen)
            request-id (uuid/gen)]
        (loop [cmds [{:request-id     request-id
                      :interaction-id interaction-id
                      :meta           {:realm (get-in ctx [:meta :realm])}
                      :commands       [{:cmd-id :cmd-level-1
                                        :id     agg-id}]}]]
          (let [effects (reduce
                         (fn [p cmd]
                           (concat
                            p
                            (-> (edd/handler (assoc ctx :no-summary true)
                                             cmd)
                                :result
                                :effects)))
                         []
                         cmds)]
            (is (= [(select-keys @counts
                                 [:fx-total
                                  :fx-error
                                  :fx-exception
                                  :fx-remaining])]
                   (get-fx-meta ctx {:request-id request-id
                                     :breadcrumbs [0]})))
            (when (seq effects)
              (recur effects))))
        (is (= (get-fx-meta ctx {:request-id request-id
                                 :breadcrumbs [0]})
               (get-fx-meta ctx {:request-id request-id
                                 :breadcrumbs [0]})))
        (is (= {:fx-remaining 0}
               (select-keys @counts [:fx-remaining])))
        (log/info "TOTAL: " @counts)
        (let [{:keys [cmd-level-2
                      cmd-level-3
                      cmd-level-4
                      cmd-level-5]} @counts]
          (is (= [{:fx-total (+ cmd-level-2
                                cmd-level-4
                                cmd-level-3
                                cmd-level-5)
                   :fx-error cmd-level-3
                   :fx-exception 0
                   :fx-remaining 0}]
                 (get-fx-meta ctx {:request-id request-id
                                   :breadcrumbs [0]})))))))
