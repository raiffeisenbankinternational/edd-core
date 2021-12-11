(ns lambda.sqs-filter-test
  (:require
   [lambda.util :as util]
   [aws.lambda :refer [handle-request]]
   [clojure.string :as str]
   [lambda.util :refer [to-edn]]
   [lambda.filters :as fl]
   [lambda.uuid :as uuid]
   [clojure.test :refer :all]
   [lambda.util :as util]
   [lambda.core :as core]
   [lambda.test.fixture.core :refer [mock-core]]
   [lambda.test.fixture.client :refer [verify-traffic-edn]]
   [clojure.test :refer :all]
   [sdk.aws.common :as common]))

(def interaction-id #uuid "0000b7b5-9f50-4dc4-86d1-2e4fe1f6d491")
(def request-id #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491")

(defn req
  [key]
  {:Records
   [{:md5OfBody
     "fda65eb3c65bf08466667a54227de6ca",
     :eventSourceARN
     "arn:aws:sqs:eu-central-1:111111111:queue-name",
     :awsRegion         "eu-central-1",
     :messageId
     "f0153036-04c9-4afd-af00-d085c49da408",
     :eventSource       "aws:sqs",
     :messageAttributes {},
     :body              (util/to-json {:Records
                                       [{:eventName         "ObjectCreated:Put",
                                         :awsRegion         "eu-central-1",
                                         :responseElements
                                         {:x-amz-request-id "61533B0E9A639258",
                                          :x-amz-id-2
                                          "DJ5rllFwuXfxfoirla3qHhcb98HkZpYoQNsnm5Q3MOPrgUUJ4IxZYVGR8Mkuzm0twkhRhlu0NHvTYF3Fuon4f5o1oi/lHdY3"},
                                         :requestParameters {:sourceIPAddress "163.116.178.120"},
                                         :userIdentity
                                         {:principalId "AWS:AROAVIIIFPLU5VJYIP3IT:john.smith@example.com"},
                                         :eventVersion      "2.1",
                                         :eventTime         "2020-02-19T06:39:03.585Z",
                                         :eventSource       "aws:s3",
                                         :s3
                                         {:s3SchemaVersion "1.0",
                                          :configurationId "92cb8ae7-173a-4965-bda8-7c5d318bdcff",
                                          :bucket
                                          {:name          "s3-bucket",
                                           :ownerIdentity {:principalId "A2MAYJCLGKVHNV"},
                                           :arn           "arn:aws:s3:::361331260137-s3-bucket"},
                                          :object
                                          {:key       key,
                                           :size      7374,
                                           :eTag      "11b66437638975195b741d99798f8296",
                                           :sequencer "005E4CD80C728F4485"}}}]})
     :receiptHandle
     "AQEBrvNGAn733odb9JN0X1N4YMzCVLYWXCCFjPx3YycpCSMaGjKyZxrbwZkz9CLtWMGVDwyJv/5sdS9N4epHvmexQVe8+lSfsGwtqFtN1zbKRjItRMO//BrZkp29BZ1LQv3zOAfCc4JJBGEINfaOpyo41Ajvqx0Dq0MwcYs8UR3RAH5kcILtsUD5NrdbR0sF0db8HmHNCZ7B875xm0dF5IB4WffEB+xH4vwoAEx8HuaBAoL2SOfUDT7+40uS8K3K+UZlzvKqOq9tfj7voCv+6Bi4e5u13S/z50e2YjHZZ00bNe4CBwNzUcDjZTtVIL/5cHEHLm8Rw8+wPwLYRkvsRt76j4M0DC7QObkgw8ixtnkBGmb5cnsXWSYZ8TraVPZkb00y1/30zl0s96VoEUSJ7qd40tBgUzSvwOcjoBOozC4AYH4=",
     :attributes
     {:ApproximateReceiveCount "1",
      :SentTimestamp           "1582094349942",
      :SenderId                "AIDAIZRJAGH76OE2BC64E",
      :ApproximateFirstReceiveTimestamp
      "1582094349946"}}]})

(defn records
  [key]
  (util/to-json (req key)))

(defn get-sq-key
  [{:keys [body]}]
  (-> body
      (:Records)
      (first)
      (:s3)
      (:object)
      (:key)))

(deftest queue-cond
  (let [resp ((:cond fl/from-queue) {:body (req "key")})]
    (is (= resp true))))

(deftest queue-fn
  (let [resp (get-sq-key
              ((:fn fl/from-queue) {:body (req "limedocu.txt")}))]
    (is (= "limedocu.txt" resp))))

(deftest test-s3-bucket-sqs-request
  (with-redefs [common/create-date (fn [] "20200426T061823Z")]
    (let [key (str "test/2021-12-27/"
                   interaction-id
                   "/"
                   request-id
                   ".limedocu.txt")]
      (mock-core
       :invocations [(records key)]
       :requests [{:get  (str "https://s3.eu-central-1.amazonaws.com/s3-bucket/"
                              key)
                   :body (char-array "Of something")}]
       (core/start
        {}
        (fn [ctx body]
          "Slurp content of S3 request into response"
          (let [commands (:commands body)
                cmd (first commands)
                response (assoc cmd :body
                                (slurp (:body cmd)))]
            (assoc body :commands [response])))
        :filters [fl/from-queue fl/from-bucket])
       (verify-traffic-edn [{:body   {:commands       [{:body   "Of something"
                                                        :cmd-id :object-uploaded
                                                        :date   "2021-12-27"
                                                        :id     request-id
                                                        :bucket "s3-bucket"
                                                        :key    key}]
                                      :meta           {:realm :test
                                                       :user  {:email "non-interractiva@s3.amazonws.com"
                                                               :id    #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491"
                                                               :role  :non-interactive}}
                                      :user           "local-test"
                                      :interaction-id interaction-id
                                      :request-id     request-id}
                             :method :post
                             :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                            {:as              :stream
                             :connect-timeout 300
                             :headers         {"Authorization"        "AWS4-HMAC-SHA256 Credential=/20200426/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=4228a5c1f0c90c90944df7658346fc5f2f0a3e365f7352b24991c6f1be6863d0"
                                               "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                                               "x-amz-date"           "20200426T061823Z"
                                               "x-amz-security-token" nil}
                             :idle-timeout    5000
                             :method          :get
                             :url             "https://s3.eu-central-1.amazonaws.com/s3-bucket/test/2021-12-27/0000b7b5-9f50-4dc4-86d1-2e4fe1f6d491/1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491.limedocu.txt"}

                            {:method  :get
                             :timeout 90000000
                             :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))
