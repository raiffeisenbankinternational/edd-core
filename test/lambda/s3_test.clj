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
   [clojure.test :refer :all]
   [clojure.tools.logging :as log]
   [sdk.aws.common :as common]
   [edd.core :as edd])
  (:import (clojure.lang ExceptionInfo)))

(def interaction-id #uuid "0000b7b5-9f50-4dc4-86d1-2e4fe1f6d491")
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

(deftest test-s3-bucket-request
  (with-redefs [common/create-date (fn [] "20200426T061823Z")]
    (let [key (str "test/"
                   interaction-id
                   "/"
                   request-id
                   ".csv")]
      (mock-core
       :invocations [(records key)]
       :requests [{:get  (str "https://s3.eu-central-1.amazonaws.com/example-bucket/"
                              key)
                   :body (char-array "Of something")}]
       (core/start
        {}
        (fn [ctx body]
          "Slurp content of S3 request into response"
          (log/info (:commands body))
          (let [commands (:commands body)
                cmd (first commands)
                response (assoc cmd :body
                                (slurp (:body cmd)))]
            (assoc body :commands [response]
                   :user (get-in ctx [:user :id])
                   :role (get-in ctx [:user :role]))))
        :filters [fl/from-bucket])
       (verify-traffic-json
        [{:body   {:commands       [{:body   "Of something"
                                     :cmd-id :object-uploaded
                                     :id     request-id
                                     :key    key}]
                   :user           "local-test"
                   :meta           {:realm :test
                                    :user  {:email "non-interractiva@s3.amazonws.com"
                                            :id    #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491"
                                            :role  :non-interactive}}
                   :role           :non-interactive
                   :interaction-id interaction-id
                   :request-id     request-id}
          :method :post
          :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
         {:as      :stream
          :headers {"Authorization"        "AWS4-HMAC-SHA256 Credential=/20200426/eu-central-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=cfa02ff06ebb0010bcfbae58ee1ec2310379fea4f66f5a0013265ccd1fde16ce"
                    "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                    "x-amz-date"           "20200426T061823Z"
                    "x-amz-security-token" nil}
          :method  :get
          :connect-timeout 200
          :idle-timeout 5000
          :url     (str "https://s3.eu-central-1.amazonaws.com/example-bucket/" key)}
         {:method  :get
          :timeout 90000000
          :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(deftest test-s3-bucket-request-when-folder-craeted
  (with-redefs [common/create-date (fn [] "20200426T061823Z")]
    (let [key (str "test/"
                   interaction-id
                   "/")]
      (mock-core
       :invocations [(records key)]
       :requests [{:get  (str "https://s3.eu-central-1.amazonaws.com/example-bucket/"
                              key)
                   :body (char-array "Of something")}]
       (core/start
        {}
        edd/handler
        :filters [fl/from-bucket])
       (verify-traffic-json
        [{:body   {}
          :method :post
          :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
         {:method  :get
          :timeout 90000000
          :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

(deftest s3-cond
  (let [resp ((:cond fl/from-bucket) {:body (util/to-edn
                                             (records "test/key"))})]
    (is (= resp true))))

(deftest test-filter-key
  (is (= {:interaction-id #uuid "af42568c-f8e9-40ff-9329-d13f1c82fce5"
          :realm          "test"
          :request-id     #uuid "af42568c-f8e9-40ff-9329-d13f1c82fca3"}
         (fl/parse-key "test/af42568c-f8e9-40ff-9329-d13f1c82fce5/af42568c-f8e9-40ff-9329-d13f1c82fca3.matching1L.csv")))
  (is (= {:interaction-id #uuid "af42568c-f8e9-40ff-9329-d13f1c82fce5"
          :realm          "prod"
          :request-id     #uuid "af42568c-f8e9-40ff-9329-d13f1c82fca3"}
         (fl/parse-key "upload/af42568c-f8e9-40ff-9329-d13f1c82fce5/af42568c-f8e9-40ff-9329-d13f1c82fca3.matching1L.csv")))
  (is (= {:interaction-id #uuid "af42568c-f8e9-40ff-9329-d13f1c82fce5"
          :realm          "prod"
          :request-id     #uuid "af42568c-f8e9-40ff-9329-d13f1c82fca3"}
         (fl/parse-key "prod/2021-08-21/af42568c-f8e9-40ff-9329-d13f1c82fce5/af42568c-f8e9-40ff-9329-d13f1c82fca3.matching1L.csv")))
  (is (thrown?
       ExceptionInfo
       (fl/parse-key "test/af42568c-f8e9-40ff-9329-d13f1c82fce5/af42568c-f8e9-40ff-9329-d13f1c82fcz3.matching1L.csv"))))

