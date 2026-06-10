(ns aws.ctx-test
  (:require
   [aws.ctx]
   [clojure.test :refer [deftest is]]
   [lambda.util :as util]))

(def resolve-credentials
  #'aws.ctx/resolve-credentials)

(defn env-stub
  [env]
  (fn
    ([name] (get env name))
    ([name default] (get env name default))))

(def endpoint-response
  {:body {:AccessKeyId "endpoint-key"
          :SecretAccessKey "endpoint-secret"
          :Token "endpoint-token"
          :Expiration "2099-01-01T00:00:00Z"}})

(deftest container-uri-preferred-over-env-keys
  ;; SnapStart freezes the env keys in the snapshot; the container endpoint
  ;; must win even when both are present.
  (with-redefs [util/get-env
                (env-stub {"AWS_ACCESS_KEY_ID" "frozen-key"
                           "AWS_SECRET_ACCESS_KEY" "frozen-secret"
                           "AWS_SESSION_TOKEN" "frozen-token"
                           "AWS_CONTAINER_CREDENTIALS_FULL_URI" "http://localhost/creds"})

                util/http-get
                (fn [_uri _opts] endpoint-response)]
    (let [creds
          (resolve-credentials {})]

      (is
       (= "endpoint-key"
          (:aws-access-key-id creds)))

      (is
       (= "endpoint-token"
          (:aws-session-token creds))))))

(deftest env-keys-used-without-container-uri
  (with-redefs [util/get-env
                (env-stub {"AWS_ACCESS_KEY_ID" "env-key"
                           "AWS_SECRET_ACCESS_KEY" "env-secret"
                           "AWS_SESSION_TOKEN" "env-token"})]
    (let [creds
          (resolve-credentials {})]

      (is
       (= "env-key"
          (:aws-access-key-id creds)))

      (is
       (= "env-token"
          (:aws-session-token creds))))))

(deftest preloaded-creds-win-over-everything
  (with-redefs [util/get-env
                (env-stub {"AWS_ACCESS_KEY_ID" "env-key"
                           "AWS_SECRET_ACCESS_KEY" "env-secret"
                           "AWS_CONTAINER_CREDENTIALS_FULL_URI" "http://localhost/creds"})]
    (let [creds
          (resolve-credentials {:aws-access-key-id "preloaded-key"})]

      (is
       (= "preloaded-key"
          (:aws-access-key-id creds))))))
