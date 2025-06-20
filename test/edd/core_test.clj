(ns edd.core-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [edd.common :as common]
            [edd.core :as edd]
            [edd.dal :as dal]
            [edd.el.cmd :as cmd]
            [edd.el.event :as event]
            [edd.el.query :as query]
            [edd.elastic.view-store :as elastic-view-store]
            [edd.memory.event-store :as event-store]
            [edd.search :as search]
            [edd.test.fixture.dal :as mock]
            [lambda.api-test :as api]
            [lambda.core :as core]
            [lambda.filters :as fl]
            [lambda.s3-test :as s3]
            [lambda.test.fixture.client :as client :refer [verify-traffic-edn]]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [sdk.aws.common :as sdk-common]
            [sdk.aws.sqs :as sqs])
  (:import (clojure.lang ExceptionInfo)))

(defn dummy-command-handler
  [ctx cmd]
  (log/info "Dummy" cmd)
  {:event-id :dummy-event
   :id       (:id cmd)
   :handled  true})

(defn dummy-command-handler-2
  [ctx cmd]
  (log/info "Dummy" cmd)
  [{:event-id :dummy-event-1
    :id       (:id cmd)
    :handled  true}
   {:event-id :dummy-event-2
    :id       (:id cmd)
    :handled  true}])

(defn dummy-command-handler-3
  [ctx cmd]
  (log/info "Dummy" cmd)
  [{:event-id :dummy-event-3
    :id       (:id cmd)
    :handled  true}])

(def fx-id #uuid "22222111-1111-1111-1111-111111111111")
(def fx-command
  {:commands [{:cmd-id :fx-command
               :id     #uuid "22222111-1111-1111-1111-111111111111"}]
   :service  :local-test})

(defn prepare
  [ctx]
  (-> mock/ctx
      (merge ctx)
      (assoc-in [:meta :realm] :test)
      (assoc :service-name :local-test)
      (edd/reg-cmd :dummy-cmd dummy-command-handler)
      (edd/reg-fx (fn [ctx events]
                    (if (some #(= (:event-id %)
                                  :dummy-event)
                              events)
                      fx-command
                      [])))
      (edd/reg-cmd :dummy-cmd-2
                   dummy-command-handler-2
                   :deps {:test-dps (fn [_ctx _cmd] {:query-id :test-query})})
      (edd/reg-cmd :dummy-cmd-3
                   dummy-command-handler-3
                   :deps {:test-dps (fn [_ctx _cmd] {:query-id :test-query})}
                   :id-fn (fn [dps cmd] (get-in dps [:test-dps :id])))
      (edd/reg-cmd :object-uploaded dummy-command-handler)
      (edd/reg-cmd :error-cmd (fn [ctx cmd]
                                (throw (ex-info "Error"
                                                {:error "Some error"}))))
      (edd/reg-query :get-by-id (fn [ctx query]
                                  (common/get-by-id (assoc ctx :id (:id query)))))
      (edd/reg-event :e7 (fn [ctx event] {:name (:event-id event)}))))

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))
(def cmd-id-2 (uuid/parse "222222-2222-2222-2222-222222222222"))
(def cmd-id-3 (uuid/parse "333333-3333-3333-3333-333333333333"))
(def dps-id (uuid/parse "dddddd-dddd-dddd-dddd-dddddddddddd"))

(def multi-command-request
  {:commands [{:cmd-id :dummy-cmd
               :id     cmd-id}
              {:cmd-id :dummy-cmd-2
               :id     cmd-id-2}
              {:cmd-id :dummy-cmd-3
               :id     cmd-id-3}]})

#_(deftest test-command-id-resolution
    (let [resp (cmd/resolve-commands-id-fn
                (merge
                 multi-command-request
                 {:id-fn {:dummy-cmd-2
                          (fn [ctx cmd] dps-id)}}))]
      (is (= {:commands [{:cmd-id :dummy-cmd
                          :id     #uuid "00111111-1111-1111-1111-111111111111"}
                         {:cmd-id      :dummy-cmd-2
                          :id          #uuid "00dddddd-dddd-dddd-dddd-dddddddddddd"
                          :original-id #uuid "00222222-2222-2222-2222-222222222222"}
                         {:cmd-id :dummy-cmd-3
                          :id     #uuid "00333333-3333-3333-3333-333333333333"}]}
             (select-keys resp [:commands])))))

(deftest handler-builder-test
  "Test if id-fn works correctly together with event seq. This test does multiple things. Sorry!!"
  (mock/with-mock-dal
    (is (= {:error "Some error"}
           (mock/handle-cmd
            (prepare {})
            {:commands [{:cmd-id :error-cmd
                         :id     cmd-id}]})))))

(deftest test-error-response
  "Test if id-fn works correctly together with event seq. This test does multiple things. Sorry!!"
  (with-redefs [event-store/get-max-event-seq-impl (fn [{:keys [id]}]
                                                     (get {cmd-id   21
                                                           dps-id   31
                                                           cmd-id-2 5
                                                           cmd-id-3 9}
                                                          id))]
    (mock/with-mock-dal
      {:event-store
       [{:event-id  :cmd-id
         :id        cmd-id
         :event-seq 21}
        {:event-id  :cmd-id
         :id        dps-id
         :event-seq 31}
        {:event-id  :cmd-id
         :id        cmd-id-2
         :event-seq 5}
        {:event-id  :cmd-id
         :id        cmd-id-3
         :event-seq 9}]}
      (let [resp (mock/handle-cmd
                  (-> (prepare {})
                      (edd/reg-query :test-query (fn [_ _] {:id dps-id})))
                  multi-command-request)]
        (mock/verify-state :event-store [{:event-id  :cmd-id
                                          :id        cmd-id-2
                                          :event-seq 5}
                                         {:event-id  :dummy-event-1
                                          :handled   true
                                          :event-seq 6
                                          :meta      {:realm :test}
                                          :id        cmd-id-2}
                                         {:event-id  :dummy-event-2
                                          :handled   true
                                          :event-seq 7
                                          :meta      {:realm :test}
                                          :id        cmd-id-2}
                                         {:event-id  :cmd-id
                                          :id        cmd-id-3
                                          :event-seq 9}
                                         {:event-id  :cmd-id
                                          :id        cmd-id
                                          :event-seq 21}
                                         {:event-id  :dummy-event
                                          :handled   true
                                          :event-seq 22
                                          :meta      {:realm :test}
                                          :id        cmd-id}
                                         {:event-id  :cmd-id
                                          :id        dps-id
                                          :event-seq 31}
                                         {:event-id  :dummy-event-3
                                          :handled   true
                                          :event-seq 32
                                          :meta      {:realm :test}
                                          :id        dps-id}])
        (is (= {:effects    [{:cmd-id       :fx-command
                              :id           #uuid "22222111-1111-1111-1111-111111111111"
                              :service-name :local-test}]
                :events     4
                :identities 0
                :meta       [{:dummy-cmd {:id cmd-id}}
                             {:dummy-cmd-2 {:id cmd-id-2}}
                             {:dummy-cmd-3 {:id dps-id}}]
                :sequences  0
                :success    true}
               resp))))))

(def request-id #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491")
(def interaction-id #uuid "2222b7b5-9f50-4dc4-86d1-2e4fe1f6d491")

(deftest test-s3-bucket-request
  (mock/with-mock-dal
    (with-redefs [sdk-common/create-date (fn [] "20200426T061823Z")
                  uuid/gen (fn [] request-id)
                  key (str "test/2021-12-27/" interaction-id "/"
                           request-id)
                  sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                    (is (= {:Records [{:key (str "response/"
                                                                 request-id
                                                                 "/0/local-test.json")}]}
                                           (util/to-edn message))))]
      (mock-core
       :invocations [(s3/records key)]
       :requests [{:get  (str "https://s3.eu-central-1.amazonaws.com/example-bucket/"
                              key)
                   :body (char-array "Of something")}]
       (core/start
        (prepare {})
        edd/handler
        :filters [fl/from-bucket])
       (verify-traffic-edn [{:body   {:result         {:effects    [{:cmd-id       :fx-command
                                                                     :id           #uuid "22222111-1111-1111-1111-111111111111"
                                                                     :service-name :local-test}]
                                                       :events     1
                                                       :identities 0
                                                       :meta       [{:object-uploaded {:id #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491"}}]
                                                       :sequences  0
                                                       :success    true}
                                      :invocation-id  0
                                      :interaction-id interaction-id
                                      :request-id     request-id}
                             :method :post
                             :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                            {:as              :stream
                             :connect-timeout 300
                             :headers         {"Authorization"        "AWS4-HMAC-SHA256 Credential=/20200426/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=a574e8bfe0d25d8565c3cc47a17f225ec5c1e246c9a7b8646c44b80ba4c50e5c"
                                               "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                                               "x-amz-date"           "20200426T061823Z"
                                               "x-amz-security-token" ""}
                             :idle-timeout    5000
                             :method          :get
                             :url             "https://s3.eu-central-1.amazonaws.com/example-bucket/test/2021-12-27/2222b7b5-9f50-4dc4-86d1-2e4fe1f6d491/1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491"}

                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(deftest test-api-request-with-fx
  "Test that user is added to events and summary is properly returned"
  (mock/with-mock-dal
    (with-redefs [sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                    (is (= {:Records [{:key (str "response/"
                                                                 request-id
                                                                 "/0/local-test.json")}]}
                                           (util/to-edn message))))]
      (mock-core
       :env {"Region" "eu-west-1"}
       :invocations [(api/api-request
                      {:commands       [{:cmd-id :dummy-cmd,
                                         :bla    "ble",
                                         :id     cmd-id}],
                       :user           {:selected-role :group-2}
                       :request-id     request-id,
                       :interaction-id interaction-id})]

       :requests [{:get  "https://s3.eu-central-1.amazonaws.com/example-bucket/test/key"
                   :body (char-array "Of something")}]
       (core/start
        (prepare {})
        edd/handler
        :filters [fl/from-api]
        :post-filter fl/to-api)
       (verify-traffic-edn [{:body   {:body            (util/to-json
                                                        {:result         {:success    true
                                                                          :effects    [{:id           fx-id
                                                                                        :cmd-id       :fx-command
                                                                                        :service-name :local-test}]
                                                                          :events     1
                                                                          :meta       [{:dummy-cmd {:id cmd-id}}]
                                                                          :identities 0
                                                                          :sequences  0}
                                                         :invocation-id  0
                                                         :request-id     request-id
                                                         :interaction-id interaction-id})
                                      :headers         {:Access-Control-Allow-Headers  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                                                        :Access-Control-Allow-Methods  "OPTIONS,POST,PUT,GET"
                                                        :Access-Control-Allow-Origin   "*"
                                                        :Access-Control-Expose-Headers "*"
                                                        :Content-Type                  "application/json"}
                                      :isBase64Encoded false
                                      :statusCode      200}
                             :method :post
                             :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(deftest test-api-request-single-command
  "Test if sending single command works on api"
  (mock/with-mock-dal
    (with-redefs [sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                    (is (= {:Records [{:key (str "response/"
                                                                 request-id
                                                                 "/0/local-test.json")}]}
                                           (util/to-edn message))))]
      (mock-core
       :env {"Region" "eu-west-1"}
       :invocations [(api/api-request
                      {:command        {:cmd-id :dummy-cmd,
                                        :bla    "ble",
                                        :id     cmd-id},
                       :user           {:selected-role :group-2}
                       :request-id     request-id,
                       :interaction-id interaction-id})]

       :requests [{:get  "https://s3.eu-central-1.amazonaws.com/example-bucket/test/key"
                   :body (char-array "Of something")}]
       (core/start
        (prepare {})
        edd/handler
        :filters [fl/from-api]
        :post-filter fl/to-api)
       (verify-traffic-edn [{:body   {:body            (util/to-json
                                                        {:result         {:success    true
                                                                          :effects    [{:id           fx-id
                                                                                        :cmd-id       :fx-command
                                                                                        :service-name :local-test}]
                                                                          :events     1
                                                                          :meta       [{:dummy-cmd {:id cmd-id}}]
                                                                          :identities 0
                                                                          :sequences  0}
                                                         :invocation-id  0
                                                         :request-id     request-id
                                                         :interaction-id interaction-id})
                                      :headers         {:Access-Control-Allow-Headers  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                                                        :Access-Control-Allow-Methods  "OPTIONS,POST,PUT,GET"
                                                        :Access-Control-Allow-Origin   "*"
                                                        :Access-Control-Expose-Headers "*"
                                                        :Content-Type                  "application/json"}
                                      :isBase64Encoded false
                                      :statusCode      200}
                             :method :post
                             :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(def apply-ctx
  {:def-apply {:event-1 (fn [p v]
                          (assoc p :e1 v))
               :event-2 (fn [p v]
                          (assoc p :e2 v))}})

(deftest test-apply
  (let [agg (event/get-current-state
             apply-ctx
             {:events [{:event-id  :event-1
                        :id        cmd-id
                        :event-seq 1
                        :k1        "a"}
                       {:event-id  :event-2
                        :id        cmd-id
                        :event-seq 2
                        :k2        "b"}]
              :id "ag1"})]
    (is (= {:id      cmd-id
            :version 2
            :e1      {:event-id  :event-1,
                      :k1        "a"
                      :event-seq 1
                      :id        cmd-id},
            :e2      {:event-id  :event-2
                      :k2        "b"
                      :event-seq 2
                      :id        cmd-id}}
           agg))))

(deftest test-get-by-id

  (with-redefs [dal/get-events (fn [_]
                                 [{:event-id  :e7
                                   :event-seq 1}])
                search/simple-search (fn [ctx] ctx)]
    (let [agg (query/handle-query
               (prepare {})
               {:query
                {:query-id :get-by-id
                 :id       "bla"}})]
      (is (= {:name    :e7
              :version 1
              :id      nil}
             (:aggregate agg))))))

(deftest test-get-by-id-and-version
  (mock/with-mock-dal
    {:aggregate-store [{:id      "1"
                        :version 1}
                       {:id      "2"
                        :version 2}
                       {:id      "1"
                        :version 3}
                       {:id      "2"
                        :version 1}]}
    (testing "get without version should return the latest aggregate"
      (is (= 3 (:version (common/get-by-id mock/ctx {:id "1"}))))
      (is (= 2 (:version (common/get-by-id mock/ctx {:id "2"})))))
    (testing "otherwise the aggregate with specified version should be returned"
      (is (= 1 (:version (common/get-by-id mock/ctx {:id "2" :version 1}))))
      (is (= 2 (:version (common/get-by-id mock/ctx {:id "2" :version 2})))))))

(deftest test-get-by-id-from-shapshot

  (mock/with-mock-dal
    {:aggregate-store [{:id      "1"
                        :value   "bla"
                        :version 3}]}

    (is (= {:id      "1"
            :value   "bla"
            :version 3}
           (common/get-by-id mock/ctx {:id "1"})))))

(deftest test-get-by-id-missing-snapshot
  "Test combinations of get-by. If aggregate snapshot return nil we apply events"
  "If no events we return nil"
  (let [CTX {:aws {:region "test"
                   :aws-session-token "test"}}]
    (mock/with-mock-dal
      (with-redefs [dal/get-events (fn [_]
                                     [])
                    util/http-get (fn [url request & {:keys [raw]}]
                                    {:status 404})]
        (let [response (query/handle-query
                        (-> (prepare CTX)
                            (elastic-view-store/register))
                        {:query
                         {:query-id :get-by-id
                          :id       "bla"}})]
          (is (contains? response :aggregate))
          (is (= nil
                 (:aggregate response))))))
    (mock/with-mock-dal
      (with-redefs [dal/get-events (fn [_]
                                     [{:event-id  :e7
                                       :event-seq 1}])
                    util/http-get (fn [url request & {:keys [raw]}]
                                    {:status 403})]
        (is (thrown? ExceptionInfo
                     (query/handle-query
                      (-> (prepare CTX)
                          (elastic-view-store/register))
                      {:query
                       {:query-id :get-by-id
                        :id       "bla"}})))))
    (mock/with-mock-dal
      (with-redefs [dal/get-events (fn [_]
                                     [{:event-id  :e7
                                       :event-seq 1
                                       :id        "bla"}])
                    util/http-get (fn [url request & {:keys [raw]}]
                                    {:status 404})]
        (let [response (query/handle-query
                        (-> (prepare CTX)
                            (elastic-view-store/register))
                        {:query
                         {:query-id :get-by-id
                          :id       "bla"}})]
          (is (contains? response :aggregate))
          (is (= {:id      "bla"
                  :name    :e7
                  :version 1}
                 (:aggregate response))))))))

(deftest test-wrap-global-not-list
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  {:a :b})]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  [{:a :b}])]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  [])]
    (is (= []
           result))))

(deftest test-wrap-global-commands
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  [{:service  "a"
                                    :commands [{:a :b}]}])]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global-commands-multiple
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  [{:service  "a"
                                    :commands [{:a :b}]}
                                   {:service  "d"
                                    :commands [{:e :f}]}])]
    (is (= [{:service  "a"
             :commands [{:a :b}]}
            {:service  "d"
             :commands [{:e :f}]}]
           result))))

(deftest test-wrap-global-commands-in-vector
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  {:service  "a"
                                   :commands [{:a :b}]})]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global-commands-as-list
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  '({:service  "a"
                                     :commands [{:a :b}]}))]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-metadata-when-result-vector
  (is (= {:result :a}
         (edd/prepare-response {:resp [{:result :a}]}))))

(deftest test-metadata-when-result-error
  (is (= {:error "Some error"}
         (edd/prepare-response {:resp [{:error "Some error"}]}))))

(deftest test-metadata-when-result-map
  (is (= [{:value :a}]
         (edd/prepare-response {:resp          [{:value :a}]
                                :queue-request true}))))
