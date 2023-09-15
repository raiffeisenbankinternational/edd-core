(ns edd.commands-batch-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [lambda.test.fixture.core :refer [mock-core]]
            [edd.core :as edd]
            [edd.el.cmd :as edd-cmd]
            [edd.common :as edd-common]
            [edd.el.ctx :as el-ctx]
            [edd.el.event :as edd-events]
            [lambda.core :as core]
            [edd.test.fixture.dal :as mock]
            [lambda.test.fixture.client :as mock-client]
            [sdk.aws.common :as common]
            [sdk.aws.sqs :as sqs]))

(def agg-id (uuid/gen))

(def req-id1 (uuid/gen))
(def req-id2 (uuid/gen))
(def req-id3 (uuid/gen)) 3

(def req-id4 (uuid/gen)) 3
(def req-id5 (uuid/gen)) 3
(def int-id (uuid/gen))

(defn req
  [items]
  {:Records
   (vec
    (map-indexed
     (fn [idx it]
       {:md5OfBody         "fff479f1b3e7bae94d4fbb22f1b2cce0",
        :eventSourceARN    "arn:aws:sqs:eu-central-1:11111111111:test-evets-queue",
        :awsRegion         "eu-central-1",
        :messageId         (str "id-" (inc idx)),
        :eventSource       "aws:sqs",
        :messageAttributes {},
        :body              (util/to-json it)
        :receiptHandle     (str "handle-" (inc idx)),
        :attributes        {:ApproximateReceiveCount "1",
                            :SentTimestamp           "1580103331238",
                            :SenderId                "AIDAISDDSWNBEXIA6J64K",
                            :ApproximateFirstReceiveTimestamp
                            "1580103331242"}})
     items))})

(def ctx
  (-> mock/ctx
      (assoc :service-name "local-test")
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            {:id       (:id cmd)
                             :event-id :event-1
                             :name     (:name cmd)}))
      (edd/reg-cmd :cmd-2 (fn [ctx cmd]
                            {:error "failed"}))
      (edd/reg-cmd :cmd-3 (fn [ctx cmd]
                            (throw (ex-info "OMG"
                                            {:failed "have I!"}))))
      (edd/reg-cmd :cmd-4 (fn [ctx cmd]
                            {:id       (:id cmd)
                             :event-id :event-4
                             :name     (:name cmd)})
                   :deps {:test (fn [_cmd _query]
                                  {:query-id :query-4})})
      (edd/reg-query :query-4 (fn [ctx quers]
                                (throw (ex-info "DEPS" {:failed "yes"}))))
      (edd/reg-event :event-1
                     (fn [agg event]
                       (merge agg
                              {:value "1"})))
      (edd/reg-event :event-2
                     (fn [agg event]
                       (merge agg
                              {:value "2"})))))

(deftest test-command-handler-returns-error
  (let [messages (atom [])]
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  sqs/sqs-publish (fn [{:keys [message]}]
                                    (swap! messages #(conj % message)))]
      (mock-core
       :invocations [(util/to-json (req
                                    [{:request-id     req-id1
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD1"}]}
                                     {:request-id     req-id2
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-2
                                                        :name   "CMD2"}]}
                                     {:request-id     req-id3
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD3"}]}]))]

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"}]
       (core/start
        ctx
        edd/handler)
       (mock-client/verify-traffic-edn [{:body   [{:result         {:success    true,
                                                                    :effects    [],
                                                                    :events     1,
                                                                    :meta       [{:cmd-1 {:id agg-id}}],
                                                                    :identities 0,
                                                                    :sequences  0}
                                                   :invocation-id  0
                                                   :request-id     req-id1
                                                   :interaction-id int-id}
                                                  {:error          [{:error "failed",
                                                                     :id    agg-id}],
                                                   :invocation-id  0
                                                   :request-id     req-id2
                                                   :interaction-id int-id}
                                                  {:result         {:success    true,
                                                                    :effects    [],
                                                                    :events     1,
                                                                    :meta       [{:cmd-1 {:id agg-id}}],
                                                                    :identities 0,
                                                                    :sequences  0}
                                                   :invocation-id  0
                                                   :request-id     req-id3
                                                   :interaction-id int-id}]
                                         :method :post
                                         :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                                        {:method  :get
                                         :timeout 90000000
                                         :url     "http://mock/2018-06-01/runtime/invocation/next"}]))
      (is (= [{:Records [{:key (str "response/"
                                    req-id3
                                    "/0/local-test.json")}]}
              {:Records [{:key (str "response/"
                                    req-id2
                                    "/0/local-test.json")}]}
              {:Records [{:key (str "response/"
                                    req-id1
                                    "/0/local-test.json")}]}]
             (map
              #(util/to-edn %)
              @messages))))))

(deftest test-all-ok
  (let [messages (atom [])]
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                    (swap! messages #(conj % message)))]
      (mock-core
       :invocations [(util/to-json (req [{:request-id     req-id4
                                          :interaction-id int-id
                                          :commands       [{:id     agg-id
                                                            :cmd-id :cmd-1
                                                            :name   "CMD1"}]}
                                         {:request-id     req-id5
                                          :interaction-id int-id
                                          :commands       [{:id     agg-id
                                                            :cmd-id :cmd-1
                                                            :name   "CMD2"}]}]))]
       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"}]
       (core/start
        ctx
        edd/handler)
       (mock-client/verify-traffic-edn [{:body   [{:result         {:success    true,
                                                                    :effects    [],
                                                                    :events     1,
                                                                    :meta       [{:cmd-1 {:id agg-id}}],
                                                                    :identities 0,
                                                                    :sequences  0}
                                                   :invocation-id  0
                                                   :request-id     req-id4,
                                                   :interaction-id int-id}
                                                  {:result         {:success    true,
                                                                    :effects    [],
                                                                    :events     1,
                                                                    :meta       [{:cmd-1 {:id agg-id}}],
                                                                    :identities 0,
                                                                    :sequences  0}
                                                   :invocation-id  0
                                                   :request-id     req-id5,
                                                   :interaction-id int-id}]
                                         :method :post
                                         :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                                        {:method  :get
                                         :timeout 90000000
                                         :url     "http://mock/2018-06-01/runtime/invocation/next"}])))
    (is (= [{:Records [{:key (str "response/"
                                  req-id5
                                  "/0/local-test.json")}]}
            {:Records [{:key (str "response/"
                                  req-id4
                                  "/0/local-test.json")}]}]
           (map
            #(util/to-edn %)
            @messages)))))

(deftest test-command-handler-exception
  (let [messages (atom [])]
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  sqs/sqs-publish (fn [{:keys [message]}]
                                    (swap! messages #(conj % message)))]
      (mock-core
       :invocations [(util/to-json (req
                                    [{:request-id     req-id1
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD1"}]}
                                     {:request-id     req-id2
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-3
                                                        :name   "CMD2"}]}
                                     {:request-id     req-id3
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD3"}]}]))]

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"}]
       (core/start
        ctx
        edd/handler)
       (is (= [{:body   [{:result         {:success    true,
                                           :effects    [],
                                           :events     1,
                                           :meta       [{:cmd-1 {:id agg-id}}],
                                           :identities 0,
                                           :sequences  0}
                          :invocation-id  0
                          :request-id     req-id1
                          :interaction-id int-id}
                         {:exception      {:failed "have I!"},
                          :invocation-id  0
                          :request-id     req-id2
                          :interaction-id int-id}
                         {:result         {:success    true,
                                           :effects    [],
                                           :events     1,
                                           :meta       [{:cmd-1 {:id agg-id}}],
                                           :identities 0,
                                           :sequences  0}
                          :invocation-id  0
                          :request-id     req-id3
                          :interaction-id int-id}]
                :method :post
                :url    "http://mock/2018-06-01/runtime/invocation/0/error"}
               {:body            (str "Action=DeleteMessageBatch&QueueUrl=https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue&"
                                      "DeleteMessageBatchRequestEntry.1.Id=id-1&"
                                      "DeleteMessageBatchRequestEntry.1.ReceiptHandle=handle-1&"
                                      "DeleteMessageBatchRequestEntry.2.Id=id-3&"
                                      "DeleteMessageBatchRequestEntry.2.ReceiptHandle=handle-3&"
                                      "Expires=2020-04-18T22%3A52%3A43PST&Version=2012-11-05")

                :method          :post
                :raw             true
                :connect-timeout 300
                :idle-timeout    5000
                :url             "https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue"
                :version         :http1.1}
               {:method  :get
                :timeout 90000000
                :url     "http://mock/2018-06-01/runtime/invocation/next"}]
              (map
               #(dissoc % :headers :keepalive)
               (mock-client/traffic-edn)))))
      (is (= [{:Records [{:key (str "response/"
                                    req-id3
                                    "/0/local-test.json")}]}
              {:Records [{:key (str "response/"
                                    req-id1
                                    "/0/local-test.json")}]}]
             (map
              #(util/to-edn %)
              @messages))))))

(deftest test-failure-of-deps-resolver
  (let [messages (atom [])]
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  sqs/sqs-publish (fn [{:keys [message]}]
                                    (swap! messages #(conj % message)))]
      (mock-core
       :invocations [(util/to-json (req
                                    [{:request-id     req-id1
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD1"}]}
                                     {:request-id     req-id2
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-4
                                                        :name   "CMD4"}]}
                                     {:request-id     req-id3
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD3"}]}]))]

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue"}]
       (core/start
        ctx
        edd/handler)
       (is  (= [{:body   [{:result         {:success    true,
                                            :effects    [],
                                            :events     1,
                                            :meta       [{:cmd-1 {:id agg-id}}],
                                            :identities 0,
                                            :sequences  0}
                           :invocation-id  0
                           :request-id     req-id1
                           :interaction-id int-id}
                          {:exception      {:failed "yes"}
                           :invocation-id  0
                           :request-id     req-id2
                           :interaction-id int-id}
                          {:result         {:success    true,
                                            :effects    [],
                                            :events     1,
                                            :meta       [{:cmd-1 {:id agg-id}}],
                                            :identities 0,
                                            :sequences  0}
                           :invocation-id  0
                           :request-id     req-id3
                           :interaction-id int-id}]
                 :method :post
                 :url    "http://mock/2018-06-01/runtime/invocation/0/error"}
                {:body            (str "Action=DeleteMessageBatch&QueueUrl=https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue&"
                                       "DeleteMessageBatchRequestEntry.1.Id=id-1&"
                                       "DeleteMessageBatchRequestEntry.1.ReceiptHandle=handle-1&"
                                       "DeleteMessageBatchRequestEntry.2.Id=id-3&"
                                       "DeleteMessageBatchRequestEntry.2.ReceiptHandle=handle-3&"
                                       "Expires=2020-04-18T22%3A52%3A43PST&Version=2012-11-05")
                 :method          :post
                 :raw             true
                 :connect-timeout 300
                 :idle-timeout    5000
                 :url             "https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue"
                 :version         :http1.1}
                {:method  :get
                 :timeout 90000000
                 :url     "http://mock/2018-06-01/runtime/invocation/next"}]
               (map
                #(dissoc % :headers :keepalive)
                (mock-client/traffic-edn)))))
      (is (= [{:Records [{:key (str "response/"
                                    req-id3
                                    "/0/local-test.json")}]}
              {:Records [{:key (str "response/"
                                    req-id1
                                    "/0/local-test.json")}]}]
             (map
              #(util/to-edn %)
              @messages))))))

(deftest test-exception-in-handler
  (let [messages (atom [])]
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  edd-cmd/handle-command (fn [_ctx _body-fn]
                                           (throw (RuntimeException.)))

                  sqs/delete-message-batch (fn [{:keys [_message]}]
                                             (throw (RuntimeException. "Deleting something")))
                  sqs/sqs-publish (fn [{:keys [_message]}]
                                    (throw (RuntimeException. "Publihsing something")))]
      (mock-core
       :invocations [(util/to-json (req
                                    [{:request-id     req-id1
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD1"}]}
                                     {:request-id     req-id2
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-4
                                                        :name   "CMD4"}]}
                                     {:request-id     req-id3
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD3"}]}]))]

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue"}]
       (core/start
        ctx
        edd/handler)
       (is  (= [{:body   [{:request-id req-id1
                           :exception "Unable to parse exception",
                           :interaction-id int-id
                           :invocation-id 0}
                          {:request-id req-id2
                           :exception "Unable to parse exception",
                           :interaction-id int-id
                           :invocation-id 0}
                          {:request-id req-id3
                           :exception "Unable to parse exception",
                           :interaction-id int-id
                           :invocation-id 0}]
                 :method :post
                 :url    "http://mock/2018-06-01/runtime/invocation/0/error"}

                {:method  :get
                 :timeout 90000000
                 :url     "http://mock/2018-06-01/runtime/invocation/next"}]
               (map
                #(dissoc % :headers :keepalive)
                (mock-client/traffic-edn))))))))

(deftest test-concurent-update
  (let [versions (atom 72)
        ctx (-> mock/ctx
                (edd/reg-cmd :cmd-1 (fn [ctx _cmd]
                                      (is (= (:version (el-ctx/get-aggregate ctx))
                                             (:version (:agg ctx)))))
                             :deps {:agg (fn [_ctx cmd]
                                           {:query-id :get-agg-by-id
                                            :id (:id cmd)})})
                (edd/reg-query :get-agg-by-id edd-common/get-by-id))]
    (with-redefs [common/create-date (fn [] "20210322T232540Z")

                  sqs/sqs-publish (fn [{:keys [_message]}]
                                    (throw (RuntimeException. "Publihsing something")))
                  edd-events/fetch-snapshot (fn [_ctx id]
                                              (swap! versions inc)
                                              {:id id
                                               :version @versions})]
      (mock/with-mock-dal
        (mock/handle-cmd ctx {:cmd-id :cmd-1
                              :id (uuid/gen)})))))
