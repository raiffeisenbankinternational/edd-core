(ns aws-test
  (:require [clojure.test :refer :all]
            [lambda.util :as utils]
            [aws.aws :as aws]
            [lambda.util-test :as util-test]
            [lambda.test.fixture.client :as client]
            [lambda.util :as util]
            [sdk.aws.common :as common])
  (:import (clojure.lang ExceptionInfo)))

(def env
  {"AWS_ACCESS_KEY_ID"     "test-key-id"
   "AWS_SECRET_ACCESS_KEY" "secret-access-key-id"
   "AWS_SESSION_TOKEN"     "session-token"})

(def auth "sws-signature")

(def ctx
  {:aws  {:aws-access-key-id     "test-key-id"
          :aws-secret-access-key "secret-access-key-id"
          :aws-session-token     "session-token"
          :region                "eu-central-1"}
   :svc  {:username "test-svc@internal"
          :password "AA33test-svc"}
   :auth {:user-pool-id  util-test/user-pool-id
          :client-id     util-test/user-pool-client-id
          :client-secret util-test/user-pool-client-secret}})

(def id-token "some-value-of-id-token")

(def login-request
  {:body            "{\"AuthFlow\":\"ADMIN_NO_SRP_AUTH\",\"AuthParameters\":{\"USERNAME\":\"test-svc@internal\",\"PASSWORD\":\"AA33test-svc\",\"SECRET_HASH\":\"CFwQXIkeWlWshlq4e+bfuUrIAoUL780KUm0JMmugvFw=\"},\"ClientId\":\"5fb55843f1doj3hjlq7ongfmn1\",\"UserPoolId\":\"eu-central-1_ACXYul00Q\"}"
   :headers         {"Authorization"        "AWS4-HMAC-SHA256 Credential=test-key-id/20200504/eu-central-1/cognito-idp/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=00ca23907f7b0b64d3e80cd8c7ebfbf13f030cbdb2b7adaa23d8f8f91d1c0da4"
                     "Content-Type"         "application/x-amz-json-1.1"
                     "X-Amz-Date"           "20200504T080055Z"
                     "X-Amz-Security-Token" "session-token"
                     "X-Amz-Target"         "AWSCognitoIdentityProviderService.AdminInitiateAuth"}
   :method          :post
   :idle-timeout    5000
   :connect-timeout 300
   :url             "https://cognito-idp.eu-central-1.amazonaws.com"})

(def auth-success-response)

(deftest test-cognito-admin-auth
  (with-redefs [utils/get-env (fn [e] (get env e))
                common/create-date (fn [] "20200504T080055Z")]
    (client/mock-http
     [{:post   "https://cognito-idp.eu-central-1.amazonaws.com"
       :status 200
       :body   (util/to-json {:AuthenticationResult
                              {:RefreshToken "refresh-token"
                               :AccessToken  "access-token"
                               :ExpiresIn    3600
                               :TokenType    "Bearer"
                               :IdToken      id-token}
                              :ChallengeParameters {}})}]
     (is (= id-token
            (aws/admin-auth ctx)))
     (client/verify-traffic
      [login-request]))))

(deftest get-token-test
  (binding [util/*cache* (atom {})]
    (with-redefs [utils/get-env (fn [e] (get env e))
                  common/create-date (fn [] "20200504T080055Z")]
      (client/mock-http
       [{:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 200
         :body   (util/to-json {:AuthenticationResult
                                {:RefreshToken "refres-token"
                                 :AccessToken  "access-token"
                                 :ExpiresIn    3600
                                 :TokenType    "Bearer"
                                 :IdToken      id-token}
                                :ChallengeParameters {}})}
        {:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 200
         :body   (util/to-json {:AuthenticationResult
                                {:RefreshToken "refresh-token"
                                 :AccessToken  "access-token"
                                 :ExpiresIn    3600
                                 :TokenType    "Bearer"
                                 :IdToken      id-token}
                                :ChallengeParameters {}})}]
       (is (= id-token
              (aws/get-token ctx)))
       ;; When this test fails on Wed Nov 13 2069 16:49:19 GMT+0100 i expect beer (as long as I still live)
       (with-redefs [util/get-current-time-ms (fn [] 3151583359900)]
         (is (= id-token
                (aws/get-token ctx))))
       (client/verify-traffic [login-request
                               login-request])))))

(deftest get-token-error-test
  (binding [util/*cache* (atom {})]
    (with-redefs [utils/get-env (fn [e] (get env e))
                  common/create-date (fn [] "20200504T080055Z")]
      (client/mock-http
       [{:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 400
         :body   (util/to-json {:message "Failed miserably"})}
        {:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 200
         :body   (util/to-json {:AuthenticationResult
                                {:RefreshToken "refresh-token"
                                 :AccessToken  "access-token"
                                 :ExpiresIn    3600
                                 :TokenType    "Bearer"
                                 :IdToken      id-token}
                                :ChallengeParameters {}})}]

       (is (= id-token
              (aws/get-token ctx)))

        ;When this test fails on Wed Mar 13 2024 16:49:19 i expect Beer

       (client/verify-traffic [login-request
                               login-request])))))

(deftest get-token-error-multiple-requests-test
  (binding [util/*cache* (atom {})]
    (with-redefs [utils/get-env (fn [e] (get env e))
                  common/create-date (fn [] "20200504T080055Z")]
      (client/mock-http
       [{:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 400
         :body   (util/to-json {:message "Failed miserably"})}
        {:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 400
         :body   (util/to-json {:message "Failed miserably"})}
        {:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 400
         :body   (util/to-json {:message "Failed miserably"})}
        {:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 400
         :body   (util/to-json {:message "Failed miserably"})}
        {:post   "https://cognito-idp.eu-central-1.amazonaws.com"
         :status 200
         :body   (util/to-json {:AuthenticationResult
                                {:RefreshToken "refresh-token"
                                 :AccessToken  "access-token"
                                 :ExpiresIn    3600
                                 :TokenType    "Bearer"
                                 :IdToken      id-token}
                                :ChallengeParameters {}})}]

       (is (thrown? ExceptionInfo
                    (aws/get-token ctx)))

       (is (= id-token
              (aws/get-token ctx)))
        ;When this test fails on Wed Mar 13 2024 16:49:19 i expect Beer

       (is (= id-token
              (aws/get-token ctx)))

       (client/verify-traffic [login-request
                               login-request
                               login-request
                               login-request
                               login-request])))))

(deftest test-adding-to-cache
  (with-redefs [util/get-current-time-ms (fn [] 1600348559351)]
    (is (= {:k1   "ski"
            :meta {:k1 {:time 1600348559351}}}
           (aws/get-or-set {} :k1 (fn [] "ski"))))))

(deftest test-old-value-if-not-expired
  (with-redefs [util/get-current-time-ms (fn [] 1600348559351)]
    (let [cache {:k1   "ski"
                 :meta {:k1 {:time 1600348559351}}}]
      (is (= cache
             (aws/get-or-set cache :k1 (fn [] "sk2")))))))

(deftest test-new-value-if-expired
  (with-redefs [util/get-current-time-ms (fn [] 1610348559351)]
    (let [cache {:k1   "ski"
                 :meta {:k1 {:time 1600348559351}}}]
      (is (= {:k1   "sk2"
              :meta {:k1 {:time 1610348559351}}}
             (aws/get-or-set cache :k1 (fn [] "sk2")))))))





