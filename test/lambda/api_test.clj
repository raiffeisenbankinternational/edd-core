(ns lambda.api-test
  (:require
   [lambda.core :refer [handle-request]]
   [clojure.string :as str]
   [lambda.util :refer [to-edn]]
   [lambda.filters :as fl]
   [lambda.util :as util]
   [clojure.test :refer :all]
   [lambda.core :as core]
   [lambda.jwt-test :as jwt-test]
   [lambda.test.fixture.client :refer [verify-traffic-json]]
   [lambda.test.fixture.core :refer [mock-core]])
  (:import (clojure.lang ExceptionInfo)))

(def cmd-id #uuid "c5c4d4df-0570-43c9-a0c5-2df32f3be124")

(def dummy-cmd
  {:commands [{:id     cmd-id
               :cmd-id :dummy-cmd}]
   :user     {:selected-role :group-2}})

(defn base64request
  [body & {:keys [token path http-method] :or {token jwt-test/token}}]
  {:path                  (or path "/path/to/resource"),
   :queryStringParameters {:foo "bar"},
   :pathParameters        {:proxy "/path/to/resource"},
   :headers
   {:Upgrade-Insecure-Requests    "1",
    :X-Amz-Cf-Id
    "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
    :CloudFront-Is-Tablet-Viewer  "false",
    :CloudFront-Forwarded-Proto   "https",
    :X-Forwarded-Proto            "https",
    :X-Forwarded-Port             "443",
    :x-authorization              token
    :Accept
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    :Accept-Encoding              "gzip, deflate, sdch",
    :X-Forwarded-For              "127.0.0.1, 127.0.0.2",
    :CloudFront-Viewer-Country    "US",
    :Accept-Language              "en-US,en;q=0.8",
    :Cache-Control                "max-age=0",
    :CloudFront-Is-Desktop-Viewer "true",
    :Via
    "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
    :CloudFront-Is-SmartTV-Viewer "false",
    :CloudFront-Is-Mobile-Viewer  "false",
    :Host
    "1234567890.execute-api.eu-central-1.amazonaws.com",
    :User-Agent                   "Custom User Agent String"},
   :stageVariables        {:baz "qux"},
   :resource              "/{proxy+}",
   :isBase64Encoded       true,
   :multiValueQueryStringParameters
   {:foo ["bar"]},
   :httpMethod            (or http-method "POST"),
   :requestContext
   {:path             "/prod/path/to/resource",
    :identity
    {:caller                        nil,
     :sourceIp                      "127.0.0.1",
     :cognitoIdentityId             nil,
     :userAgent                     "Custom User Agent String",
     :cognitoAuthenticationProvider nil,
     :accessKey                     nil,
     :accountId                     nil,
     :user                          nil,
     :cognitoAuthenticationType     nil,
     :cognitoIdentityPoolId         nil,
     :userArn                       nil},
    :stage            "prod",
    :protocol         "HTTP/1.1",
    :resourcePath     "/{proxy+}",
    :resourceId       "123456",
    :requestTime      "09/Apr/2015:12:34:56 +0000",
    :requestId
    "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
    :httpMethod       "POST",
    :requestTimeEpoch 1428582896000,
    :accountId        "123456789012",
    :apiId            "1234567890"},
   :body                  (util/base64encode (util/to-json body)),
   :multiValueHeaders
   {:Upgrade-Insecure-Requests    ["1"],
    :X-Amz-Cf-Id
    ["cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA=="],
    :CloudFront-Is-Tablet-Viewer  ["false"],
    :CloudFront-Forwarded-Proto   ["https"],
    :X-Forwarded-Proto            ["https"],
    :X-Forwarded-Port             ["443"],
    :Accept
    ["text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"],
    :Accept-Encoding              ["gzip, deflate, sdch"],
    :X-Forwarded-For              ["127.0.0.1, 127.0.0.2"],
    :CloudFront-Viewer-Country    ["US"],
    :Accept-Language              ["en-US,en;q=0.8"],
    :Cache-Control                ["max-age=0"],
    :CloudFront-Is-Desktop-Viewer ["true"],
    :Via
    ["1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)"],
    :CloudFront-Is-SmartTV-Viewer ["false"],
    :CloudFront-Is-Mobile-Viewer  ["false"],
    :Host
    ["0123456789.execute-api.eu-central-1.amazonaws.com"],
    :User-Agent                   ["Custom User Agent String"]}})

(defn request
  [body & {:keys [token path http-method] :or {token jwt-test/token}}]
  {:path                  (or path "/path/to/resource"),
   :queryStringParameters {:foo "bar"},
   :pathParameters        {:proxy "/path/to/resource"},
   :headers
   {:Upgrade-Insecure-Requests    "1",
    :X-Amz-Cf-Id
    "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
    :CloudFront-Is-Tablet-Viewer  "false",
    :CloudFront-Forwarded-Proto   "https",
    :X-Forwarded-Proto            "https",
    :X-Forwarded-Port             "443",
    :x-authorization              token
    :Accept
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    :Accept-Encoding              "gzip, deflate, sdch",
    :X-Forwarded-For              "127.0.0.1, 127.0.0.2",
    :CloudFront-Viewer-Country    "US",
    :Accept-Language              "en-US,en;q=0.8",
    :Cache-Control                "max-age=0",
    :CloudFront-Is-Desktop-Viewer "true",
    :Via
    "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
    :CloudFront-Is-SmartTV-Viewer "false",
    :CloudFront-Is-Mobile-Viewer  "false",
    :Host
    "1234567890.execute-api.eu-central-1.amazonaws.com",
    :User-Agent                   "Custom User Agent String"},
   :stageVariables        {:baz "qux"},
   :resource              "/{proxy+}",
   :isBase64Encoded       false,
   :multiValueQueryStringParameters
   {:foo ["bar"]},
   :httpMethod            (or http-method "POST"),
   :requestContext
   {:path             "/prod/path/to/resource",
    :identity
    {:caller                        nil,
     :sourceIp                      "127.0.0.1",
     :cognitoIdentityId             nil,
     :userAgent                     "Custom User Agent String",
     :cognitoAuthenticationProvider nil,
     :accessKey                     nil,
     :accountId                     nil,
     :user                          nil,
     :cognitoAuthenticationType     nil,
     :cognitoIdentityPoolId         nil,
     :userArn                       nil},
    :stage            "prod",
    :protocol         "HTTP/1.1",
    :resourcePath     "/{proxy+}",
    :resourceId       "123456",
    :requestTime      "09/Apr/2015:12:34:56 +0000",
    :requestId
    "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
    :httpMethod       "POST",
    :requestTimeEpoch 1428582896000,
    :accountId        "123456789012",
    :apiId            "1234567890"},
   :body                  (util/to-json body),
   :multiValueHeaders
   {:Upgrade-Insecure-Requests    ["1"],
    :X-Amz-Cf-Id
    ["cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA=="],
    :CloudFront-Is-Tablet-Viewer  ["false"],
    :CloudFront-Forwarded-Proto   ["https"],
    :X-Forwarded-Proto            ["https"],
    :X-Forwarded-Port             ["443"],
    :Accept
    ["text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"],
    :Accept-Encoding              ["gzip, deflate, sdch"],
    :X-Forwarded-For              ["127.0.0.1, 127.0.0.2"],
    :CloudFront-Viewer-Country    ["US"],
    :Accept-Language              ["en-US,en;q=0.8"],
    :Cache-Control                ["max-age=0"],
    :CloudFront-Is-Desktop-Viewer ["true"],
    :Via
    ["1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)"],
    :CloudFront-Is-SmartTV-Viewer ["false"],
    :CloudFront-Is-Mobile-Viewer  ["false"],
    :Host
    ["0123456789.execute-api.eu-central-1.amazonaws.com"],
    :User-Agent                   ["Custom User Agent String"]}})

(defn api-request
  [& params]
  (util/to-json (apply request params)))

(defn api-request-base64
  [& params]
  (util/to-json (apply base64request params)))

(deftest has-role-test
  (is (= nil
         (fl/has-role? {:roles []} nil))))

(deftest api-handler-test
  (mock-core
   :invocations [(api-request dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (:user ctx)})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (do
     (verify-traffic-json
      [{:body   {:body            (util/to-json
                                   {:source dummy-cmd
                                    :user   {:id    "john.smith@example.com"
                                             :email "john.smith@example.com",
                                             :role  :group-2
                                             :roles [:anonymous :group-3 :group-2 :group-1]}})
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
        :url     "http://mock/2018-06-01/runtime/invocation/next"}]))))

(deftest api-handler-test-base64
  (mock-core
   :invocations [(api-request-base64 dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (:user ctx)})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (do
     (verify-traffic-json
      [{:body   {:body            (util/to-json
                                   {:source dummy-cmd
                                    :user   {:id    "john.smith@example.com"
                                             :email "john.smith@example.com",
                                             :role  :group-2
                                             :roles [:anonymous :group-3 :group-2 :group-1]}})
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
        :url     "http://mock/2018-06-01/runtime/invocation/next"}]))))

(deftest api-handler-invalid-token-test
  (mock-core
   :invocations [(api-request dummy-cmd :token "")]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (:user ctx)})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-json
    [{:body   {:body            (util/to-json
                                 {:error {:jwt :invalid}})
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
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-error-result
  (mock-core
   :invocations [(api-request dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:error "Some error"})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-json
    [{:body   {:body            (util/to-json
                                 {:error "Some error"})
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
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-handler-exception
  (mock-core
   :invocations [(api-request dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      (throw (new RuntimeException "Some error")))
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-json
    [{:body   {:body            (util/to-json
                                 {:error "Some error"})
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
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-bucket-filter-ignored
  (mock-core
   :invocations [(api-request dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:source body})
    :filters [fl/from-bucket]
    :post-filter fl/to-api)
   (verify-traffic-json
    [{:body   {:body            (util/to-json
                                 {:source (request dummy-cmd)})
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
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-health-check
  (mock-core
   :invocations [(api-request dummy-cmd
                              :path "/health")]
   (core/start
    {}
    (fn [ctx body]
      {:healthy false})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-json
    [{:body   {:body            (util/to-json
                                 {:healthy  true
                                  :build-id "b0"})
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
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-options-check
  (mock-core
   :invocations [(api-request dummy-cmd
                              :http-method "OPTIONS")]
   (core/start
    {}
    (fn [ctx body]
      {:healthy false})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-json
    [{:body   {:body            (util/to-json
                                 {:healthy  true
                                  :build-id "b0"})
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
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-custom-config
  (mock-core
   :env {"CustomConfig" (util/to-json
                         {:a :b
                          :c :d})}
   :invocations [(util/to-json {})]
   (core/start
    {}
    (fn [ctx body]
      (is (= {:a :b
              :c :d}
             (select-keys ctx [:a :c])))
      {:source body
       :user   (:user ctx)}))
   (do)))

(deftest test-realm-filter
  (is (= :test
         (fl/get-realm {} {:roles [:some-role :realm-test]} :some-role)))
  (is (thrown? ExceptionInfo
               (fl/get-realm {} {:roles [:some-role]} :some-role))))