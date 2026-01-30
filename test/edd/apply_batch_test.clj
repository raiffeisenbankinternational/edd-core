(ns edd.apply-batch-test
  (:require [clojure.test :refer [deftest testing]]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [lambda.test.fixture.core :refer [mock-core]]
            [edd.core :as edd]
            [lambda.core :as core]
            [edd.memory.event-store :as event-store]
            [edd.elastic.view-store :as view-store]
            [lambda.test.fixture.client :as client]
            [edd.el.event :as event]
            [sdk.aws.common :as common]))

(def agg-id #uuid "05120289-90f3-423c-ad9f-c46f9927a53e")

(def req-id1 (uuid/gen))
(def req-id2 (uuid/gen))
(def req-id3 (uuid/gen)) 3

(def int-id (uuid/gen))

(def account-id "11111111111")

(defn req
  [items]
  {:Records
   (vec
    (map-indexed
     (fn [idx it]
       {:md5OfBody         "fff479f1b3e7bae94d4fbb22f1b2cce0",
        :eventSourceARN    (str "arn:aws:sqs:eu-central-1:" account-id ":test-evets-queue"),
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
      (assoc :service-name :local-test
             :hosted-zone-name "example.com"
             :environment-name-lower "local"
             :meta {:realm :test})
      (event-store/register)
      (view-store/register)
      (edd/reg-cmd :cmd-1 (fn [_ctx cmd]
                            {:id       (:id cmd)
                             :event-id :event-1
                             :name     (:name cmd)}))
      (edd/reg-cmd :cmd-2 (fn [_ctx _cmd]
                            {:error "failed"}))
      (edd/reg-event :event-1
                     (fn [agg _event]
                       (merge agg
                              {:value "1"})))
      (edd/reg-event :event-2
                     (fn [_agg _event]
                       (throw (ex-info "Sory" {:something "happened"}))))))

(deftest apply-when-two-events-1
  (testing "Ensure that wee only udpate aggregate once for same aggregate id"
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  event/get-by-id (fn [_ctx id]
                                    {:id id :version 1})]
      (mock-core
       :env {"AccountId" account-id
             "EnvironmentNameLower" "local"}
       :invocations [(util/to-json (req
                                    [{:apply          {:service      "glms-booking-company-svc",
                                                       :aggregate-id agg-id}
                                      :meta           {:realm :test}
                                      :request-id     req-id1
                                      :interaction-id int-id}
                                     {:apply          {:service      "glms-booking-company-svc",
                                                       :aggregate-id agg-id}

                                      :meta           {:realm :test}
                                      :request-id     req-id2
                                      :interaction-id int-id}
                                     {:apply          {:service      "glms-booking-company-svc",
                                                       :aggregate-id agg-id}
                                      :request-id     req-id3
                                      :meta           {:realm :test}
                                      :interaction-id int-id}]))]

       :requests [{:put (str "https://" account-id "-local-aggregates.s3.eu-central-1.amazonaws.com/aggregates/test/history/local-test/1110/"
                             agg-id
                             "/1.json")
                   :status 200
                   :body (char-array "OK")}
                  {:put (str "https://" account-id "-local-aggregates.s3.eu-central-1.amazonaws.com/aggregates/test/latest/local-test/1110/"
                             agg-id
                             ".json")
                   :status 200
                   :body (char-array "OK")}
                  {:post   (str "https://127.0.0.1:9200/test_local_test/_doc/" agg-id)
                   :status 200}
                  {:post "http://mock/2018-06-01/runtime/invocation/0/response"
                   :status 200}]
       (core/start
        ctx
        edd/handler)
       ;; verify-traffic-edn checks traffic in chronological order:
       ;; 1. Lambda GET next, 2. S3 history PUT, 3. S3 latest PUT, 4. ES POST, 5. Lambda response POST
       ;; Note: SQS delete only happens on error path, not success path
       (client/verify-traffic-edn [{:method  :get
                                    :timeout 90000000
                                    :url     "http://mock/2018-06-01/runtime/invocation/next"}
                                   {:url
                                    (str "https://" account-id "-local-aggregates.s3.eu-central-1.amazonaws.com/aggregates/test/history/local-test/1110/"
                                         agg-id
                                         "/1.json")
                                    :idle-timeout 5000,
                                    :connect-timeout 300
                                    :method :put,
                                    :body {:service-name :local-test,
                                           :realm "test",
                                           :aggregate {:id agg-id :version 1}}
                                    :headers
                                    {"Authorization"
                                     "AWS4-HMAC-SHA256 Credential=/20210322/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=a8c681be45c9259d0331b2420c91a74a61fa327e5525f88bdc81ecf13339e739"
                                     "x-amz-security-token" "",
                                     "x-amz-date" "20210322T232540Z",
                                     "x-amz-content-sha256" "UNSIGNED-PAYLOAD"},
                                    :as :stream}
                                   {:url
                                    (str "https://" account-id "-local-aggregates.s3.eu-central-1.amazonaws.com/aggregates/test/latest/local-test/1110/"
                                         agg-id
                                         ".json")
                                    :idle-timeout 5000,
                                    :connect-timeout 300
                                    :method :put,
                                    :body {:service-name :local-test,
                                           :realm "test",
                                           :aggregate {:id agg-id :version 1}}
                                    :headers
                                    {"Authorization"
                                     "AWS4-HMAC-SHA256 Credential=/20210322/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=00c6eed9407ed42b76125d2b412667c0b60b6c4a6a211b09be7d4620c3deb713"
                                     "x-amz-security-token" "",
                                     "x-amz-date" "20210322T232540Z",
                                     "x-amz-content-sha256" "UNSIGNED-PAYLOAD"},
                                    :as :stream}
                                   {:body            {:id agg-id :version 1}
                                    :headers         {"Authorization"
                                                      "AWS4-HMAC-SHA256 Credential=/20210322/eu-central-1/es/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=0638c5ee883f1fbaf6ad17f492f32d4d3d8832e24d28d06e2967ee3fedb57795"
                                                      "Content-Type"         "application/json"
                                                      "X-Amz-Date"           "20210322T232540Z"
                                                      "X-Amz-Security-Token" ""}
                                    :method          :post
                                    :idle-timeout    20000
                                    :connect-timeout 300
                                    :url             "https://127.0.0.1:9200/test_local_test/_doc/05120289-90f3-423c-ad9f-c46f9927a53e"}
                                   {:body   [{:result         {:apply true}
                                              :invocation-id  0
                                              :request-id     req-id1,
                                              :interaction-id int-id}
                                             {:result         {:apply true}
                                              :invocation-id  0
                                              :request-id     req-id2,
                                              :interaction-id int-id}
                                             {:result         {:apply true}
                                              :invocation-id  0
                                              :request-id     req-id3,
                                              :interaction-id int-id}]
                                    :method :post
                                    :url    "http://mock/2018-06-01/runtime/invocation/0/response"}])))))

(deftest apply-when-error-all-failed
  (with-redefs [common/create-date (fn [] "20210322T232540Z")
                event/get-by-id (fn [ctx id]
                                  (when (= (:request-id ctx)
                                           req-id2)
                                    (throw (ex-info "Something" {:badly :1wrong})))
                                  (when (= (:request-id ctx)
                                           req-id3)
                                    (throw (RuntimeException. "Non clojure error")))
                                  {:id id :version 1})
                event/update-aggregate (fn [ctx _aggregate]
                                         (when (= (:request-id ctx)
                                                  req-id1)
                                           (throw (ex-info "Something" {:badly :un-happy}))))]
    (mock-core
     :env {"AccountId" account-id}
     :invocations [(util/to-json (req
                                  [{:apply          {:service      "glms-booking-company-svc",
                                                     :aggregate-id agg-id}
                                    :request-id     req-id1
                                    :interaction-id int-id}
                                   {:apply          {:service      "glms-booking-company-svc",
                                                     :aggregate-id agg-id}
                                    :request-id     req-id2
                                    :interaction-id int-id}
                                   {:apply          {:service      "glms-booking-company-svc",
                                                     :aggregate-id agg-id}
                                    :request-id     req-id3
                                    :interaction-id int-id}]))]

     :requests [{:post (str "https://sqs.eu-central-1.amazonaws.com/" account-id "/test-evets-queue")}
                {:post   (str "https:///local_test/_doc/" agg-id)
                 :status 200}
                {:post   (str "https:///local_test/_doc/" agg-id)
                 :status 200}
                {:post   (str "https:///local_test/_doc/" agg-id)
                 :status 200}
                {:post "http://mock/2018-06-01/runtime/invocation/0/error"
                 :status 200}]
     (core/start
      ctx
      edd/handler)
     (client/verify-traffic-edn [{:method  :get
                                  :timeout 90000000
                                  :url     "http://mock/2018-06-01/runtime/invocation/next"}
                                 {:body   [{:exception {:badly :un-happy}
                                            :invocation-id  0
                                            :request-id     req-id1,
                                            :interaction-id int-id}]
                                  :method :post
                                  :url    "http://mock/2018-06-01/runtime/invocation/0/error"}]))))
