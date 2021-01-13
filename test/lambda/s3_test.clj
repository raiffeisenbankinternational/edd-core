(ns lambda.s3-test
  (:require
   [lambda.util :as util]
   [lambda.core :refer [handle-request]]
   [clojure.string :as str]
   [lambda.util :refer [to-edn]]
   [lambda.filters :as fl]
   [lambda.uuid :as uuid]
   [lambda.core :as core]
   [lambda.util :as util]
   [lambda.test.fixture.core :refer [mock-core]]
   [lambda.test.fixture.client :refer [verify-traffic-json]]
   [clojure.test :refer :all]))

(def request-id #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491")

(defn records
  [key]
  (util/to-json
   {:Records
    [{:eventName         "ObjectCreated:Put",
      :awsRegion         "eu-central-1",
      :responseElements
      {:x-amz-request-id "EXAMPLE123456789",
       :x-amz-id-2
       "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"},
      :requestParameters {:sourceIPAddress "127.0.0.1"},
      :userIdentity      {:principalId "EXAMPLE"},
      :eventVersion      "2.0",
      :eventTime         "1970-01-01T00:00:00.000Z",
      :eventSource       "aws:s3",
      :s3
      {:s3SchemaVersion "1.0",
       :configurationId "testConfigRule",
       :bucket
       {:name          "example-bucket",
        :ownerIdentity {:principalId "EXAMPLE"},
        :arn           "arn:aws:s3:::example-bucket"},
       :object
       {:key       key
        :size      1024,
        :eTag      "0123456789abcdef0123456789abcdef",
        :sequencer "0A1B2C3D4E5F678901"}}}]}))

(def base-requests
  [{:as      :stream
    :headers {"Authorization"        "AWS4-HMAC-SHA256 Credential=/20200426/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=459105a47452ef09fa31cc0d1c74212390d00d0a7bcaf4ee34aac83da39124e9"
              "Host"                 "s3.eu-central-1.amazonaws.com"
              "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
              "x-amz-date"           "20200426T061823Z"
              "x-amz-security-token" nil}
    :method  :get
    :timeout 8000
    :url     "https://s3.eu-central-1.amazonaws.com/example-bucket/test/key"}
   {:method  :get
    :timeout 90000000
    :url     "http://mock/2018-06-01/runtime/invocation/next"}])

(deftest test-s3-bucket-request
  (with-redefs [aws/create-date (fn [] "20200426T061823Z")
                uuid/gen (fn [] request-id)]
    (mock-core
     :invocations [(records "test/key")]
     :requests [{:get  "https://s3.eu-central-1.amazonaws.com/example-bucket/test/key"
                 :body (char-array "Of something")}]
     (core/start
      {}
      (fn [ctx body]
        "Slurp content of S3 request into response"
        (let [commands (:commands body)
              cmd (first commands)
              response (assoc cmd :body
                              (slurp (:body cmd)))]
          (assoc body :commands [response]
                 :user (get-in ctx [:user :id])
                 :role (get-in ctx [:user :role]))))
      :filters [fl/from-bucket])
     (verify-traffic-json (cons
                           {:body   {:commands       [{:body   "Of something"
                                                       :cmd-id :object-uploaded
                                                       :id     request-id
                                                       :key    "test/key"}]
                                     :user "local-test"
                                     :role :non-interactive
                                     :interaction-id request-id
                                     :request-id     request-id}
                            :method :post
                            :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                           base-requests)))))

(deftest s3-cond
  (let [resp ((:cond fl/from-bucket) {:body (util/to-edn
                                             (records "test/key"))})]
    (is (= resp true))))







