(ns edd.commands-batch-test
  (:require
   [aws.aws :as aws]
   [clojure.test :refer :all]
   [edd.common :as edd-common]
   [edd.core :as edd]
   [edd.el.cmd :as edd-cmd]
   [edd.el.ctx :as el-ctx]
   [edd.el.event :as edd-events]
   [edd.memory.event-store :as memory]
   [edd.test.fixture.dal :as mock]
   [lambda.core :as core]
   [lambda.filters :as lambda-filters]
   [lambda.test.fixture.client :as mock-client]
   [lambda.test.fixture.core :refer [mock-core]]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [sdk.aws.common :as common]
   [sdk.aws.sqs :as sqs])
  (:import
   (java.sql SQLTransientConnectionException)))

(def agg-id (uuid/gen))

(defn gen-req-uuid
  [num]
  (let [rid (uuid/gen)
        rid (str rid)
        rid (subs rid 3)
        rid (str num rid)]
    (uuid/parse rid)))

(def req-id1 (gen-req-uuid "01a"))
(def req-id2 (gen-req-uuid "02a"))
(def req-id3 (gen-req-uuid "03a")) 3
(def req-id4 (gen-req-uuid "04a")) 3
(def req-id5 (gen-req-uuid "05a")) 3

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
      (edd/reg-cmd :cmd-with-handler-error (fn [ctx cmd]
                                             {:error "failed"}))
      (edd/reg-cmd :cmd-with-handler-exception (fn [ctx cmd]
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
                                                        :cmd-id :cmd-with-handler-error
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
                                                        :cmd-id :cmd-with-handler-error
                                                        :name   "CMD2"}]}
                                     {:request-id     req-id3
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-with-handler-exception
                                                        :name   "CMD2"}]}
                                     {:request-id     req-id4
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
                         {:error      [{:id agg-id
                                        :error "failed"}]
                          :invocation-id  0
                          :request-id     req-id2
                          :interaction-id int-id}
                         {:request-id req-id3
                          :exception {:failed "have I!"},
                          :interaction-id int-id,
                          :invocation-id 0}]
                :method :post
                :url    "http://mock/2018-06-01/runtime/invocation/0/error"}
               {:body            (str "Action=DeleteMessageBatch&QueueUrl=https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue&"
                                      "DeleteMessageBatchRequestEntry.1.Id=id-1&"
                                      "DeleteMessageBatchRequestEntry.1.ReceiptHandle=handle-1&"
                                      "DeleteMessageBatchRequestEntry.2.Id=id-2&"
                                      "DeleteMessageBatchRequestEntry.2.ReceiptHandle=handle-2&"
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
                                    req-id2
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
                           :interaction-id int-id}]
                 :method :post
                 :url    "http://mock/2018-06-01/runtime/invocation/0/error"}
                {:body            (str "Action=DeleteMessageBatch&QueueUrl=https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue&"
                                       "DeleteMessageBatchRequestEntry.1.Id=id-1&"
                                       "DeleteMessageBatchRequestEntry.1.ReceiptHandle=handle-1&"
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
                           :invocation-id 0}]
                 :method :post
                 :url    "http://mock/2018-06-01/runtime/invocation/0/error"}

                {:method  :get
                 :timeout 90000000
                 :url     "http://mock/2018-06-01/runtime/invocation/next"}]
               (map
                #(dissoc % :headers :keepalive)
                (mock-client/traffic-edn))))))))

(deftest test-get-items-to-delete
  (testing "Function should stop at the first exception"
    (is (= ["rec1" "rec2"]
           (aws/get-items-to-delete
            [{:data "data1"} {:data "data2"} {:exception true}]
            ["rec1" "rec2" "rec3"]))))

  (testing "Function should return all records associated with non-exceptional responses"
    (is (= ["rec1" "rec2"]
           (aws/get-items-to-delete
            [{:data "data1"} {:data "data2"}]
            ["rec1" "rec2"]))))

  (testing "Function should also check if more record then responses"
    (is (= ["rec1" "rec2"]
           (aws/get-items-to-delete
            [{:data "data1"} {:data "data2"}]
            ["rec1" "rec2" "rec3"]))))

  (testing "Function should not delete if record is missing"
    (is (= ["rec1"]
           (aws/get-items-to-delete
            [{:data "data1"} {:data "data1"} {:exception true}]
            ["rec1"]))))

  (testing "Function should cope with empty lists"
    (is (empty? (aws/get-items-to-delete [] []))))

  (testing "The number of records returned should match the number of non-exceptional responses"
    (is (let [responses [{} {} {:exception true}]
              records [1 2 3 4]]
          (and (= 2 (count (aws/get-items-to-delete responses records)))
               (not= records (aws/get-items-to-delete responses records)))))))

(deftest send-error-test
  (let [responses (atom [])
        deletes (atom [])]
    (testing "send-error without from-api"
      (with-redefs [util/http-post (fn [_url req]
                                     (swap! responses conj req)
                                     {:status 200})
                    sqs/delete-message-batch (fn [{:keys [messages]}]
                                               (swap! deletes concat messages))]
        (let [ctx {:api "some-api"
                   :invocation-id "123"
                   :from-api false
                   :req {:Records [{:receiptHandle "handle1"
                                    :eventSourceARN "arn:aws:sqs:region:account-id:queue-name"}
                                   {:receiptHandle "handle2"
                                    :eventSourceARN "arn:aws:sqs:region:account-id:queue-name"}
                                   {:receiptHandle "handle2"
                                    :eventSourceARN "arn:aws:sqs:region:account-id:queue-name"}]}}
              body [{:data {}}
                    {:exception true}
                    {:data {}}]]
          (aws/send-error ctx body)))

      (is (= [{:receiptHandle "handle1",
               :eventSourceARN "arn:aws:sqs:region:account-id:queue-name"}]
             @deletes))
      (is (= 1
             (count @deletes))))))

(deftest send-error-when-single-request-test
  (let [responses (atom [])
        deletes (atom [])]
    (testing "send-error without from-api"
      (with-redefs [util/http-post (fn [_url req]
                                     (swap! responses conj req)
                                     {:status 200})
                    sqs/delete-message-batch (fn [{:keys [messages]}]
                                               (swap! deletes concat messages))]
        (let [ctx {:api "some-api"
                   :invocation-id "123"
                   :from-api false
                   :req {:Records [{:receiptHandle "handle1"
                                    :eventSourceARN "arn:aws:sqs:region:account-id:queue-name"}]}}
              body {:exception true}]
          (aws/send-error ctx body)))

      (is (= []
             @deletes))
      (is (= 0
             (count @deletes))))))

(deftest test-exception-command-request-log
  (let [messages (atom [])

        exceptions (atom [(RuntimeException. "No connections 0")
                          (ex-info "No connections 1" {:error "No connections 1"})
                          (SQLTransientConnectionException. "No connections 2")
                          (ex-info "No connections 1" {:error "No connections 3"})])]
    (with-redefs [common/create-date
                  (fn [] "20210322T232540Z")

                  memory/log-request-impl
                  (fn [_ctx _body-fn]
                    (let [e (first @exceptions)]
                      (swap! exceptions rest)
                      (throw e)))

                  sqs/delete-message-batch
                  (fn [{:keys [message]}]
                    (swap! messages conj message))

                  sqs/sqs-publish
                  (fn [{:keys [_message]}]
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
                                                        :name   "CMD3"}]}
                                     {:request-id     req-id4
                                      :interaction-id int-id
                                      :commands       [{:id     agg-id
                                                        :cmd-id :cmd-1
                                                        :name   "CMD4"}]}]))]

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/local/test-evets-queue"}]

       (core/start
        ctx
        edd/handler
        :post-filter lambda-filters/to-api)

       (do
         (is  (= [{:body   [{:request-id req-id1
                             :exception "No connections 0"
                             :interaction-id int-id
                             :invocation-id 0}]
                   :method :post
                   :url    "http://mock/2018-06-01/runtime/invocation/0/error"}

                  {:method  :get
                   :timeout 90000000
                   :url     "http://mock/2018-06-01/runtime/invocation/next"}]
                 (map
                  #(dissoc % :headers :keepalive)
                  (mock-client/traffic-edn))))
         (is (= 0
                (count @messages))))))))

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
