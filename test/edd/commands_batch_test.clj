(ns edd.commands-batch-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [lambda.test.fixture.client :refer [verify-traffic]]
            [lambda.test.fixture.core :refer [mock-core]]
            [edd.core :as edd]
            [lambda.core :as core]
            [edd.test.fixture.dal :as mock]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.client :as client]))

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
  (-> {}
      (assoc :service-name "local-test")
      (event-store/register)
      (view-store/register)
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            {:id       (:id cmd)
                             :event-id :event-1
                             :name     (:name cmd)}))
      (edd/reg-cmd :cmd-2 (fn [ctx cmd]
                            {:error "failed"}))
      (edd/reg-event :event-1
                     (fn [agg event]
                       (merge agg
                              {:value "1"})))
      (edd/reg-event :event-2
                     (fn [agg event]
                       (merge agg
                              {:value "2"})))))

(deftest apply-when-two-events-1
  (with-redefs [aws/create-date (fn [] "20210322T232540Z")]
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
     (verify-traffic [{:body   (util/to-json
                                [{:success        true,
                                  :effects        [],
                                  :events         1,
                                  :meta           [{:cmd-1 {:id agg-id}}],
                                  :identities     0,
                                  :sequences      0,
                                  :request-id     req-id1
                                  :interaction-id int-id}
                                 {:error          [{:error "failed",
                                                    :id    agg-id}],
                                  :request-id     req-id2
                                  :interaction-id int-id}
                                 {:success        true,
                                  :effects        [],
                                  :events         1,
                                  :meta           [{:cmd-1 {:id agg-id}}],
                                  :identities     0,
                                  :sequences      0,
                                  :request-id     req-id3
                                  :interaction-id int-id}])
                       :method :post
                       :url    "http://mock/2018-06-01/runtime/invocation/0/error"}
                      {:body    (str "Action=DeleteMessageBatch&QueueUrl=https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue&"
                                     "DeleteMessageBatchRequestEntry.1.Id=id-1&"
                                     "DeleteMessageBatchRequestEntry.1.ReceiptHandle=handle-1&"
                                     "DeleteMessageBatchRequestEntry.2.Id=id-3&"
                                     "DeleteMessageBatchRequestEntry.2.ReceiptHandle=handle-3&"
                                     "Expires=2020-04-18T22%3A52%3A43PST&Version=2012-11-05")
                       :headers {"Accept"               "application/json"
                                 "Authorization"        "AWS4-HMAC-SHA256 Credential=/20210322/eu-central-1/sqs/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date, Signature=15c52699656249eb14aadde7232a50e8e6ac9dfa3cca36e54a7ec071495b4dd8"
                                 "Content-Type"         "application/x-www-form-urlencoded"
                                 "X-Amz-Date"           "20210322T232540Z"
                                 "X-Amz-Security-Token" nil}
                       :method  :post
                       :raw     true
                       :timeout 5000
                       :url     "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"
                       :version :http1.1}
                      {:method  :get
                       :timeout 90000000
                       :url     "http://mock/2018-06-01/runtime/invocation/next"}]))))

(deftest apply-when-two-events-2
  (with-redefs [aws/create-date (fn [] "20210322T232540Z")]
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
     (verify-traffic [{:body   (util/to-json
                                [{:success        true,
                                  :effects        [],
                                  :events         1,
                                  :meta           [{:cmd-1 {:id agg-id}}],
                                  :identities     0,
                                  :sequences      0,
                                  :request-id     req-id4,
                                  :interaction-id int-id}
                                 {:success        true,
                                  :effects        [],
                                  :events         1,
                                  :meta           [{:cmd-1 {:id agg-id}}],
                                  :identities     0,
                                  :sequences      0,
                                  :request-id     req-id5,
                                  :interaction-id int-id}])
                       :method :post
                       :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                      {:method  :get
                       :timeout 90000000
                       :url     "http://mock/2018-06-01/runtime/invocation/next"}]))))




