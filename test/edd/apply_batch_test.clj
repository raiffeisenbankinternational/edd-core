(ns edd.apply-batch-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [lambda.test.fixture.client :refer [verify-traffic-edn]]
            [lambda.test.fixture.core :refer [mock-core]]
            [edd.core :as edd]
            [lambda.core :as core]
            [edd.test.fixture.dal :as mock]
            [edd.memory.event-store :as event-store]
            [edd.elastic.view-store :as view-store]
            [lambda.test.fixture.client :as client]
            [edd.el.event :as event]
            [sdk.aws.common :as common]))

(def agg-id #uuid "05120289-90f3-423c-ad9f-c46f9927a53e")

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
      (assoc :service-name "local-test"
             :meta {:realm :test})
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
                       (throw (ex-info "Sory" {:something "happened"}))))))

(deftest apply-when-two-events-1
  (testing "Ensure that wee only udpate aggregate once for same aggregate id"
    (with-redefs [common/create-date (fn [] "20210322T232540Z")
                  event/get-by-id (fn [ctx]
                                    (assoc ctx
                                           :aggregate {:id agg-id}))]
      (mock-core
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

       :requests [{:post "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"}
                  {:post   (str "https:///test_local_test/_doc/" agg-id)
                   :status 200}
                  {:post   (str "https:///test_local_test/_doc/" agg-id)
                   :status 200}
                  {:post   (str "https:///test_local_test/_doc/" agg-id)
                   :status 200}
                  {:put (str "https://s3.eu-central-1.amazonaws.com/--aggregates/aggregates/test/latest/local-test/1110/"
                             agg-id
                             ".json")
                   :status 200
                   :body (char-array "OK")}]
       (core/start
        ctx
        edd/handler)
       (verify-traffic-edn [{:body   [{:result         {:apply true}
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
                             :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                            {:body            {:id agg-id}
                             :headers         {"Authorization"        "AWS4-HMAC-SHA256 Credential=/20210322/eu-central-1/es/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=8b2d9b4b2390562f95edc1b2dc52223e9cac7eb1b50b460156d53183ef2346e3"
                                               "Content-Type"         "application/json"
                                               "X-Amz-Date"           "20210322T232540Z"
                                               "X-Amz-Security-Token" ""}
                             :method          :post
                             :idle-timeout    20000
                             :connect-timeout 300
                             :url             "https:///test_local_test/_doc/05120289-90f3-423c-ad9f-c46f9927a53e"}
                            {:url
                             (str "https://s3.eu-central-1.amazonaws.com/--aggregates/aggregates/test/latest/local-test/1110/"
                                  agg-id
                                  ".json")
                             :idle-timeout 5000,
                             :connect-timeout 300
                             :method :put,
                             :body {:service-name :local-test,
                                    :realm "test",
                                    :aggregate {:id agg-id}}
                             :headers
                             {"Authorization"
                              "AWS4-HMAC-SHA256 Credential=/20210322/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=b4cd223d1e2593c779cc5bececcf4921363eb3d889708f54eb30163e7a5b12a0"
                              "x-amz-security-token" nil,
                              "x-amz-date" "20210322T232540Z",
                              "x-amz-content-sha256" "UNSIGNED-PAYLOAD"},
                             :as :stream}
                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(deftest apply-when-error-all-failed
  (with-redefs [common/create-date (fn [] "20210322T232540Z")
                event/get-by-id (fn [ctx]
                                  (when (= (:request-id ctx)
                                           req-id2)
                                    (throw (ex-info "Something" {:badly :1wrong})))
                                  (when (= (:request-id ctx)
                                           req-id3)
                                    (throw (RuntimeException. "Non clojure error")))
                                  (assoc ctx
                                         :aggregate {:id agg-id}))
                event/update-aggregate (fn [ctx]
                                         (if (= (:request-id ctx)
                                                req-id1)
                                           (throw (ex-info "Something" {:badly :un-happy}))))]
    (mock-core
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

     :requests [{:post "https://sqs.eu-central-1.amazonaws.com/11111111111/test-evets-queue"}
                {:post   (str "https:///local_test/_doc/" agg-id)
                 :status 200}
                {:post   (str "https:///local_test/_doc/" agg-id)
                 :status 200}
                {:post   (str "https:///local_test/_doc/" agg-id)
                 :status 200}]
     (core/start
      ctx
      edd/handler)
     (verify-traffic-edn [{:body   [{:error          {:badly :un-happy}
                                     :invocation-id  0
                                     :request-id     req-id1,
                                     :interaction-id int-id}
                                    {:error          {:badly :1wrong}
                                     :invocation-id  0
                                     :request-id     req-id2,
                                     :interaction-id int-id}
                                    {:error          "Non clojure error"
                                     :invocation-id  0
                                     :request-id     req-id3,
                                     :interaction-id int-id}]
                           :method :post
                           :url    "http://mock/2018-06-01/runtime/invocation/0/error"}
                          {:method  :get
                           :timeout 90000000
                           :url     "http://mock/2018-06-01/runtime/invocation/next"}]))))







