(ns edd.postgres.event-store-it
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as event-store]
            [edd.commands-batch-test :as batch-test]
            [lambda.test.fixture.client :refer [verify-traffic-edn]]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.state :refer [*dal-state*]]
            [edd.core :as edd]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [lambda.test.fixture.core :as core-mock]
            [lambda.util :as util]
            [edd.common :as common]
            [lambda.logging :as lambda-logging]
            [clojure.tools.logging :as log]
            [edd.el.cmd :as cmd]
            [lambda.core :as core])
  (:import (clojure.lang ExceptionInfo)))

(def fx-id (uuid/gen))

(defn- with-realm
  [ctx]
  (assoc-in ctx [:meta :realm] :test))

(defn get-ctx
  [invocation-id]
  (-> {}
      (assoc :service-name "local-test")
      (assoc :invocation-id invocation-id)
      (assoc :response-cache :default)
      (assoc :environment-name-lower (util/get-env "EnvironmentNameLower"))
      (assoc :aws {:region                (util/get-env "AWS_DEFAULT_REGION")
                   :account-id            (util/get-env "AccountId")
                   :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID")
                   :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY")
                   :aws-session-token     (util/get-env "AWS_SESSION_TOKEN")})
      (event-store/register)
      (view-store/register)
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            [{:identity (:id cmd)}
                             {:sequence (:id cmd)}
                             {:id       (:id cmd)
                              :event-id :event-1
                              :name     (:name cmd)}
                             {:id       (:id cmd)
                              :event-id :event-2
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

(deftest test-transaction-when-saving
  (with-redefs [event-store/store-cmd (fn [ctx cmd]
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
                       (event-store/get-response-log (with-realm ctx) invocation-id)))))

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
                               :dps {:seq edd.common/get-sequence-number-for-id})
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
                        (event-store/get-response-log (with-realm ctx) invocation-id)))
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
          expected-error [{:id ["missing required key"]}]]

      (edd/with-stores
        ctx (fn [ctx]
              (is (= [{:error expected-error}]
                     (mapv :error (event-store/get-request-log (with-realm ctx) request-id ""))))))

      (is (= {:exception      expected-error
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
