(ns lambda.api-test
  (:require [clojure.test :refer [deftest is]]
            [lambda.core :as core]
            [lambda.filters :as fl]
            [lambda.jwt-test :as jwt-test]
            [lambda.test.fixture.client :as client :refer [verify-traffic-edn]]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.util :as util])
  (:import
   clojure.lang.ExceptionInfo))

(def cmd-id #uuid "c5c4d4df-0570-43c9-a0c5-2df32f3be124")

(def dummy-cmd
  {:commands [{:id     cmd-id
               :cmd-id :dummy-cmd}]
   :user     {:selected-role :group-2}})

(def token-with-groups "eyJraWQiOiJYNXFKM3Z5ZEJHeCtoT1Jvb1hDOVlrbWpxQzU4aUU3SzVKVnBQWWcrOWpvPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI4OTljMGM1Ny02NDZlLTQyOWQtYTVhNi1iZjhkZWM3YzRhODYiLCJhdWQiOiIxZW4xdmJjNnMxazBjcHZoaDBydGc1ZzFkOCIsImNvZ25pdG86Z3JvdXBzIjpbInJvbGVzLWdyb3VwLTIiLCJyb2xlcy1ncm91cC0zIiwicmVhbG0tcHJvZCIsInJvbGVzLWdyb3VwLTEiXSwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV2ZW50X2lkIjoiZTNlYjIxMjEtOGIwYy00MWQ4LWI3ZWYtZjAyMTJjOWRkNDI1IiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE2NTA5ODkxNTUsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC5ldS13ZXN0LTEuYW1hem9uYXdzLmNvbVwvZXUtd2VzdC0xX3h3QVpFbGc2UCIsImNvZ25pdG86dXNlcm5hbWUiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIiwiZXhwIjoxNjUwOTkyNzU1LCJpYXQiOjE2NTA5ODkxNTUsImVtYWlsIjoiam9obi5zbWl0aEBleGFtcGxlLmNvbSJ9.C8rek8gX1PvYR-wnSDCGx6s15ucS5mD-7J1mQMcNS5ZhMaBDwMFqHVNORGhlgrPolnAl1u76ytSoORmtgAfBR9mf9NwqpTVj3eAMskl_aoFR603WNa2w8SFtcPVLexgOp_kQADVBrGhdPOoftASNsCobf6EpdlyiidUsu7BMal9WhyRI1yPt_Ou4WGxusy-Ojuif_Ef6C_fGv3g6ySDjTV7A_cTA-VMietwIQ6e2N2I6l9uhg4lQxWMrZlN19YTLJF6aI6BRGzjur-CLN0SosmMB7DEZAUD6lQVUwdVLRnUeOp2xVJWW7crLan5VoB9TzHMEjhppiTRwEVnfvRguyA")

(defn base64request
  [body & {:keys [token path http-method] :or {token token-with-groups}}]
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
  [body & {:keys [token path http-method] :or {token token-with-groups}}]
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

(defn m2m_request
  [body & {:keys [token path http-method] :or {token jwt-test/token}}]
  {:path                            "/integration/canary/event-log",
   :queryStringParameters           "None",
   :pathParameters                  {:stage "canary", :function "event-log"},
   :headers
   {:x-amzn-tls-version     "TLSv1.2",
    :X-Forwarded-Proto      "https",
    :X-Forwarded-Port       "443",
    :x-authorization        token,
    :Accept                 "*/*",
    :x-amzn-cipher-suite    "ECDHE-RSA-AES128-GCM-SHA256",
    :Accept-Encoding        "gzip, deflate, br",
    :X-Forwarded-For        "10.223.44.173, 10.223.128.204, 10.223.128.218",
    :x-amzn-vpc-id          "vpc-08466e8fa1fc4d330",
    :x-amzn-vpce-id         "vpce-0a42e0e4f6052d2a6",
    :x-amzn-vpce-config     "0",
    :Host                   "api.lime-dev12.internal.rbigroup.cloud",
    :Content-Type           "application/json",
    :Postman-Token          "fbfe1147-6459-454d-a6d7-68629a2022e2",
    :x-amzn-vpce-policy-url "MQ==;vpce-svc-01fedad8f6ddc0953",
    :User-Agent             "PostmanRuntime/7.29.0",
    :X-Amzn-Trace-Id
    "Self=1-624c4641-39061598649a512f2b364894;Root=1-624c4641-31a8bde2177ec9aa0409676b"},
   :stageVariables                  {:EnvironmentNameUpper "DEV12"},
   :resource                        "/integration/{stage}/{function}",
   :isBase64Encoded                 false,
   :multiValueQueryStringParameters "None",
   :httpMethod                      (or http-method "POST"),
   :requestContext
   {:path              "/integration/canary/event-log",
    :identity
    {:caller                        "None",
     :vpceId                        "vpce-0a42e0e4f6052d2a6",
     :sourceIp                      "10.223.128.218",
     :principalOrgId                "None",
     :cognitoIdentityId             "None",
     :vpcId                         "vpc-08466e8fa1fc4d330",
     :userAgent                     "PostmanRuntime/7.29.0",
     :cognitoAuthenticationProvider "None",
     :accessKey                     "None",
     :accountId                     "None",
     :user                          "None",
     :cognitoAuthenticationType     "None",
     :cognitoIdentityPoolId         "None",
     :userArn                       "None"},
    :stage             "DEV12",
    :protocol          "HTTP/1.1",
    :resourcePath      "/integration/{stage}/{function}",
    :domainPrefix      "api",
    :resourceId        "2a2tlx",
    :requestTime       "05/Apr/2022:13:38:09 +0000",
    :requestId         "7e42b0a7-465e-4f43-aaf0-95e049fcf05b",
    :domainName        "api.lime-dev12.internal.rbigroup.cloud",
    :authorizer
    {:cognito:groups     "realm-test,roles-users",
     :user               "rbi-glms-m2m-prod"
     :token_use          "m2m"
     :email              "rbi-glms-m2m-prod@rbi.cloud"
     :integrationLatency 207},
    :httpMethod        "POST",
    :requestTimeEpoch  1649165889404,
    :accountId         "421990764474",
    :extendedRequestId "QG_qQHmhFiAFsLQ=",
    :apiId             "t4rh2tqly8"},
   :body                            (util/to-json body),
   :multiValueHeaders
   {:x-amzn-tls-version     ["TLSv1.2"],
    :X-Forwarded-Proto      ["https"],
    :X-Forwarded-Port       ["443"],
    :x-authorization
    ["eyJhbGciOiJSUzUxMiIsImtpZCI6IkJEWWpZZXk2eUo3LTZWZWFjUkZlT2dPcVZuayIsInBpLmF0bSI6ImF1cHQifQ.eyJjbGllbnRfaWQiOiJyYmktZ2xtcy1tMm0tcHJvZCIsImlzcyI6Imh0dHBzOi8vaWRwLnJiaW50ZXJuYXRpb25hbC5jb20iLCJqdGkiOiJyV0xjZENuaVMycVM4M1EyR3BvdCIsInN1YiI6InJiaS1nbG1zLW0ybS1wcm9kIiwiaWF0IjoxNjQ5MTY1ODYyLCJzY29wZSI6Im0ybSBSQkktR0xNUy1QLVJlYWxtLVRlc3QgUkJJLUdMTVMtUC1Vc2VycyIsImV4cCI6MTY0OTE2NzY2Mn0.OtOVoqV9N9WVhbbI20nUNSi3iy4gM8o-FRrE-kEvrSAcTutvSNDcccsD9pPajwOrRXqViGxQ4l_3iwC5zz6o5hFVu3AifRsnDwUJ89Q7uJVZbfUUdXTFb2l0Z4aER0TJvICFhfIIN8bEXPhiKqWgzkAOLCTJZvbSwb2pilzqj8BZnLND90eB3MTH-0mLJUsfFTD6NO26TDk-VVWsiCy5ZVT9hluzMxVIITYoo4PIIcMOB75JFqh3R0Uuq6iI4L2Nro9hPMEozGXnuX2a3TDuo9nRhu9HuM3tfUnj4OAfP3japMW8Nsy6qtikKrsImAGDpH-d3eBzfuUxX23h8REsOA"],
    :Accept                 ["*/*"],
    :x-amzn-cipher-suite    ["ECDHE-RSA-AES128-GCM-SHA256"],
    :Accept-Encoding        ["gzip, deflate, br"],
    :X-Forwarded-For        ["10.223.44.173, 10.223.128.204, 10.223.128.218"],
    :x-amzn-vpc-id          ["vpc-08466e8fa1fc4d330"],
    :x-amzn-vpce-id         ["vpce-0a42e0e4f6052d2a6"],
    :x-amzn-vpce-config     ["0"],
    :Host                   ["api.lime-dev12.internal.rbigroup.cloud"],
    :Content-Type           ["application/json"],
    :Postman-Token          ["fbfe1147-6459-454d-a6d7-68629a2022e2"],
    :x-amzn-vpce-policy-url ["MQ==;vpce-svc-01fedad8f6ddc0953"],
    :User-Agent             ["PostmanRuntime/7.29.0"],
    :X-Amzn-Trace-Id
    ["Self=1-624c4641-39061598649a512f2b364894;Root=1-624c4641-31a8bde2177ec9aa0409676b"]}})

(defn cognito-authorizer-request
  [body & {:keys [token path http-method] :or {token jwt-test/token}}]
  {:path                            "/integration/canary/event-log",
   :queryStringParameters           "None",
   :pathParameters                  {:stage "canary", :function "event-log"},
   :headers
   {:x-amzn-tls-version     "TLSv1.2",
    :X-Forwarded-Proto      "https",
    :X-Forwarded-Port       "443",
    :x-authorization        token,
    :Accept                 "*/*",
    :x-amzn-cipher-suite    "ECDHE-RSA-AES128-GCM-SHA256",
    :Accept-Encoding        "gzip, deflate, br",
    :X-Forwarded-For        "10.223.44.173, 10.223.128.204, 10.223.128.218",
    :x-amzn-vpc-id          "vpc-08466e8fa1fc4d330",
    :x-amzn-vpce-id         "vpce-0a42e0e4f6052d2a6",
    :x-amzn-vpce-config     "0",
    :Host                   "api.lime-dev12.internal.rbigroup.cloud",
    :Content-Type           "application/json",
    :Postman-Token          "fbfe1147-6459-454d-a6d7-68629a2022e2",
    :x-amzn-vpce-policy-url "MQ==;vpce-svc-01fedad8f6ddc0953",
    :User-Agent             "PostmanRuntime/7.29.0",
    :X-Amzn-Trace-Id
    "Self=1-624c4641-39061598649a512f2b364894;Root=1-624c4641-31a8bde2177ec9aa0409676b"},
   :stageVariables                  {:EnvironmentNameUpper "DEV12"},
   :resource                        "/integration/{stage}/{function}",
   :isBase64Encoded                 false,
   :multiValueQueryStringParameters "None",
   :httpMethod                      (or http-method "POST"),
   :requestContext
   {:path              "/integration/canary/event-log",
    :identity
    {:caller                        "None",
     :vpceId                        "vpce-0a42e0e4f6052d2a6",
     :sourceIp                      "10.223.128.218",
     :principalOrgId                "None",
     :cognitoIdentityId             "None",
     :vpcId                         "vpc-08466e8fa1fc4d330",
     :userAgent                     "PostmanRuntime/7.29.0",
     :cognitoAuthenticationProvider "None",
     :accessKey                     "None",
     :accountId                     "None",
     :user                          "None",
     :cognitoAuthenticationType     "None",
     :cognitoIdentityPoolId         "None",
     :userArn                       "None"},
    :stage             "DEV12",
    :protocol          "HTTP/1.1",
    :resourcePath      "/integration/{stage}/{function}",
    :domainPrefix      "api",
    :resourceId        "2a2tlx",
    :requestTime       "05/Apr/2022:13:38:09 +0000",
    :requestId         "7e42b0a7-465e-4f43-aaf0-95e049fcf05b",
    :domainName        "api.lime-dev12.internal.rbigroup.cloud",
    :authorizer        {:claims {:cognito:groups     "realm-test,roles-users",
                                 :user               "rbi-glms-m2m-prod"
                                 :token_use          "id"
                                 :email              "rbi-glms-m2m-prod@rbi.cloud"
                                 :integrationLatency 207}},
    :httpMethod        "POST",
    :requestTimeEpoch  1649165889404,
    :accountId         "421990764474",
    :extendedRequestId "QG_qQHmhFiAFsLQ=",
    :apiId             "t4rh2tqly8"},
   :body                            (util/to-json body),
   :multiValueHeaders
   {:x-amzn-tls-version     ["TLSv1.2"],
    :X-Forwarded-Proto      ["https"],
    :X-Forwarded-Port       ["443"],
    :x-authorization
    ["eyJhbGciOiJSUzUxMiIsImtpZCI6IkJEWWpZZXk2eUo3LTZWZWFjUkZlT2dPcVZuayIsInBpLmF0bSI6ImF1cHQifQ.eyJjbGllbnRfaWQiOiJyYmktZ2xtcy1tMm0tcHJvZCIsImlzcyI6Imh0dHBzOi8vaWRwLnJiaW50ZXJuYXRpb25hbC5jb20iLCJqdGkiOiJyV0xjZENuaVMycVM4M1EyR3BvdCIsInN1YiI6InJiaS1nbG1zLW0ybS1wcm9kIiwiaWF0IjoxNjQ5MTY1ODYyLCJzY29wZSI6Im0ybSBSQkktR0xNUy1QLVJlYWxtLVRlc3QgUkJJLUdMTVMtUC1Vc2VycyIsImV4cCI6MTY0OTE2NzY2Mn0.OtOVoqV9N9WVhbbI20nUNSi3iy4gM8o-FRrE-kEvrSAcTutvSNDcccsD9pPajwOrRXqViGxQ4l_3iwC5zz6o5hFVu3AifRsnDwUJ89Q7uJVZbfUUdXTFb2l0Z4aER0TJvICFhfIIN8bEXPhiKqWgzkAOLCTJZvbSwb2pilzqj8BZnLND90eB3MTH-0mLJUsfFTD6NO26TDk-VVWsiCy5ZVT9hluzMxVIITYoo4PIIcMOB75JFqh3R0Uuq6iI4L2Nro9hPMEozGXnuX2a3TDuo9nRhu9HuM3tfUnj4OAfP3japMW8Nsy6qtikKrsImAGDpH-d3eBzfuUxX23h8REsOA"],
    :Accept                 ["*/*"],
    :x-amzn-cipher-suite    ["ECDHE-RSA-AES128-GCM-SHA256"],
    :Accept-Encoding        ["gzip, deflate, br"],
    :X-Forwarded-For        ["10.223.44.173, 10.223.128.204, 10.223.128.218"],
    :x-amzn-vpc-id          ["vpc-08466e8fa1fc4d330"],
    :x-amzn-vpce-id         ["vpce-0a42e0e4f6052d2a6"],
    :x-amzn-vpce-config     ["0"],
    :Host                   ["api.lime-dev12.internal.rbigroup.cloud"],
    :Content-Type           ["application/json"],
    :Postman-Token          ["fbfe1147-6459-454d-a6d7-68629a2022e2"],
    :x-amzn-vpce-policy-url ["MQ==;vpce-svc-01fedad8f6ddc0953"],
    :User-Agent             ["PostmanRuntime/7.29.0"],
    :X-Amzn-Trace-Id
    ["Self=1-624c4641-39061598649a512f2b364894;Root=1-624c4641-31a8bde2177ec9aa0409676b"]}})

(defn api-request
  [& params]
  (util/to-json (apply request params)))

(defn api-request-base64
  [& params]
  (util/to-json (apply base64request params)))

(defn api-request-m2m
  [& params]
  (util/to-json (apply m2m_request params)))

(defn api-request-cognito
  [& params]
  (util/to-json (apply cognito-authorizer-request params)))

(deftest has-role-test
  (is (= nil
         (fl/has-role? {:roles []} nil))))

(deftest test-m2m-authentication
  (mock-core
   :env {"Region" "eu-west-1"}
   :invocations [(api-request-m2m (assoc dummy-cmd
                                         :user {:selected-role :users}))]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (get-in ctx [:meta :user])})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-edn
    [{:body
      {:headers
       {:Access-Control-Allow-Headers  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        :Access-Control-Expose-Headers "*"
        :Access-Control-Allow-Methods  "OPTIONS,POST,PUT,GET"
        :Access-Control-Allow-Origin   "*"
        :Content-Type                  "application/json"}
       :isBase64Encoded false
       :body
       (util/to-json
        {:source
         {:commands
          [{:id     #uuid "c5c4d4df-0570-43c9-a0c5-2df32f3be124"
            :cmd-id :dummy-cmd}]
          :user
          {:selected-role :users}}
         :user {:id    "rbi-glms-m2m-prod@rbi.cloud"
                :roles [:users]
                :role  :users
                :email "rbi-glms-m2m-prod@rbi.cloud"}})
       :statusCode      200}
      :method :post
      :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
     {:timeout 90000000
      :method  :get
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest test-cognito-authorizer
  (mock-core
   :env {"Region" "eu-west-1"}
   :invocations [(api-request-cognito (assoc dummy-cmd
                                             :user {:selected-role :users}))]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (get-in ctx [:meta :user])})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-edn
    [{:body
      {:headers
       {:Access-Control-Allow-Headers  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        :Access-Control-Expose-Headers "*"
        :Access-Control-Allow-Methods  "OPTIONS,POST,PUT,GET"
        :Access-Control-Allow-Origin   "*"
        :Content-Type                  "application/json"}
       :isBase64Encoded false
       :body
       (util/to-json
        {:source
         {:commands
          [{:id     #uuid "c5c4d4df-0570-43c9-a0c5-2df32f3be124"
            :cmd-id :dummy-cmd}]
          :user
          {:selected-role :users}}
         :user {:id    "rbi-glms-m2m-prod@rbi.cloud"
                :roles [:users]
                :role  :users
                :email "rbi-glms-m2m-prod@rbi.cloud"}})
       :statusCode      200}
      :method :post
      :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
     {:timeout 90000000
      :method  :get
      :url     "http://mock/2018-06-01/runtime/invocation/next"}])))

(deftest api-handler-test
  (mock-core
   :env {"Region" "eu-west-1"}
   :invocations [(api-request dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (get-in ctx [:meta :user])
       :realm  (get-in ctx [:meta :realm])})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-edn
    [{:body   {:body            (util/to-json
                                 {:source dummy-cmd
                                  :user   {:id    "john.smith@example.com"
                                           :roles [:group-1 :group-3 :group-2]
                                           :role  :group-2
                                           :email "john.smith@example.com"}
                                  :realm  :test})
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

(deftest api-handler-test-base64
  (mock-core
   :env {"Region" "eu-west-1"}
   :invocations [(api-request-base64 dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:source body
       :user   (get-in ctx [:meta :user])})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (do
     (verify-traffic-edn
      [{:body   {:body            (util/to-json
                                   {:source dummy-cmd
                                    :user   {:id    "john.smith@example.com"
                                             :roles [:group-1 :group-3 :group-2]
                                             :role  :group-2
                                             :email "john.smith@example.com"}})
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
   (verify-traffic-edn
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
   :env {"Region" "eu-west-1"}
   :invocations [(api-request dummy-cmd)]
   (core/start
    {}
    (fn [ctx body]
      {:error "Some error"})
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-edn
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
   :env {"Region" "eu-west-1"}
   (core/start
    {}
    (fn [ctx body]
      (throw (new RuntimeException "Some error")))
    :filters [fl/from-api]
    :post-filter fl/to-api)
   (verify-traffic-edn
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
   (verify-traffic-edn
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
   (verify-traffic-edn
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
   (verify-traffic-edn
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
