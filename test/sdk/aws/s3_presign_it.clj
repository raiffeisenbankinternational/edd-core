(ns sdk.aws.s3-presign-it
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.tools.logging :as log]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [sdk.aws.s3 :as s3]))

(def test-aws-config
  {:aws-access-key-id (System/getenv "AWS_ACCESS_KEY_ID")
   :aws-secret-access-key (System/getenv "AWS_SECRET_ACCESS_KEY")
   :aws-session-token (System/getenv "AWS_SESSION_TOKEN")
   :region (or (System/getenv "AWS_REGION") "eu-central-1")})

(def test-bucket
  (str (System/getenv "TARGET_ACCOUNT_ID") "-pipeline-it"))

(deftest presign-url-integration-test
  (testing "presigned URL for PUT and GET operations"
    (let [ctx {:aws test-aws-config}
          test-uuid (str (uuid/gen))
          test-key (str "it/presign-test-" test-uuid ".json")
          test-data {:id test-uuid
                     :message "This is a test file for presigned URL functionality"
                     :timestamp (System/currentTimeMillis)
                     :nested {:value 42 :name "test"}}
          test-content (util/to-json test-data)]

      (log/info "Starting presigned URL integration test with bucket:" test-bucket "key:" test-key)

      (let [put-object {:s3 {:bucket {:name test-bucket}
                             :object {:key test-key
                                      :content test-content}}}

            put-result (s3/put-object ctx put-object)]

        (log/info "S3 put-object result:" put-result)
        (is (not (:error put-result)) "Should successfully put test object")

        (let [presigned-get-url (s3/presign-url test-aws-config
                                                test-bucket
                                                (str "/" test-key)
                                                3600
                                                {:method :get})]

          (log/info "Generated presigned GET URL:" presigned-get-url)
          (is (string? presigned-get-url) "Should generate a presigned URL string")
          (is (.contains presigned-get-url "X-Amz-Algorithm=AWS4-HMAC-SHA256")
              "Should contain AWS4 signature algorithm")
          (is (.contains presigned-get-url "X-Amz-Expires=3600")
              "Should contain expiration time")

          (let [response (util/http-get presigned-get-url {})]
            (log/info "GET response status:" (:status response) "body type:" (type (:body response)))
            (is (= 200 (:status response)) "Should successfully fetch via presigned URL")
            (is (= test-data (:body response))
                "Content should match what was uploaded")))

        (let [upload-uuid (str (uuid/gen))
              upload-key (str "it/presign-upload-" upload-uuid ".json")
              upload-data {:id upload-uuid
                           :message "This is uploaded via presigned PUT URL"
                           :timestamp (System/currentTimeMillis)}
              upload-content (util/to-json upload-data)
              presigned-put-url (s3/presign-url test-aws-config
                                                test-bucket
                                                (str "/" upload-key)
                                                3600
                                                {:method :put})]

          (log/info "Generated presigned PUT URL:" presigned-put-url)
          (log/info "Upload data:" upload-data)
          (is (string? presigned-put-url) "Should generate a presigned PUT URL")
          (is (.contains presigned-put-url "X-Amz-Algorithm=AWS4-HMAC-SHA256")
              "PUT URL should contain AWS4 signature algorithm")

          (let [upload-response (util/http-put presigned-put-url
                                               {:body upload-content
                                                :headers {"Content-Type" "application/json"}}
                                               :raw true)]
            (log/info "PUT upload response status:" (:status upload-response))
            (is (< (:status upload-response) 300) "Should successfully upload via presigned PUT URL"))

          (let [verify-get-url (s3/presign-url test-aws-config
                                               test-bucket
                                               (str "/" upload-key)
                                               3600
                                               {:method :get})
                verify-response (util/http-get verify-get-url {})]
            (log/info "Verification GET response status:" (:status verify-response) "body:" (:body verify-response))
            (is (= 200 (:status verify-response)) "Should successfully fetch uploaded file")
            (is (= upload-data (:body verify-response))
                "Downloaded content should match uploaded data"))

          (let [delete-upload-object {:s3 {:bucket {:name test-bucket}
                                           :object {:key upload-key}}}
                delete-upload-result (s3/delete-object ctx delete-upload-object)]
            (log/info "Delete upload object result:" delete-upload-result)
            (is (not (:error delete-upload-result)) "Should successfully delete uploaded test object")))

        (let [delete-object {:s3 {:bucket {:name test-bucket}
                                  :object {:key test-key}}}
              delete-result (s3/delete-object ctx delete-object)]
          (log/info "Delete test object result:" delete-result)
          (is (not (:error delete-result)) "Should successfully delete test object"))))))

(deftest presign-url-validation-test
  (testing "presigned URL parameter validation"
    (let [test-uuid (str (uuid/gen))
          test-key (str "it/validation-test-" test-uuid ".json")
          short-expiry-url (s3/presign-url test-aws-config
                                           test-bucket
                                           (str "/" test-key)
                                           60)]
      (log/info "Generated short expiry URL:" short-expiry-url)
      (is (.contains short-expiry-url "X-Amz-Expires=60")
          "Should respect custom expiration time"))

    (let [test-uuid (str (uuid/gen))
          test-key (str "it/method-test-" test-uuid ".json")
          test-data {:id test-uuid
                     :message "Test data for presigned URL validation"
                     :timestamp (System/currentTimeMillis)
                     :validation true}
          test-content (util/to-json test-data)
          get-url (s3/presign-url test-aws-config
                                  test-bucket
                                  (str "/" test-key)
                                  3600
                                  {:method :get})
          put-url (s3/presign-url test-aws-config
                                  test-bucket
                                  (str "/" test-key)
                                  3600
                                  {:method :put})]

      (log/info "Validation test - GET URL:" get-url)
      (log/info "Validation test - PUT URL:" put-url)
      (log/info "Validation test data:" test-data)

      (is (not= get-url put-url) "GET and PUT URLs should be different")
      (is (.contains get-url "X-Amz-Algorithm=AWS4-HMAC-SHA256")
          "GET URL should contain AWS4 signature algorithm")
      (is (.contains put-url "X-Amz-Algorithm=AWS4-HMAC-SHA256")
          "PUT URL should contain AWS4 signature algorithm")

      (let [upload-response (util/http-put
                             put-url
                             {:body test-content
                              :headers {"Content-Type" "application/json"}}
                             :raw true)]
        (log/info "Validation PUT response status:" (:status upload-response))
        (is (< (:status upload-response) 300) "Should successfully upload via presigned PUT URL"))

      (let [download-response (util/http-get get-url {})]
        (log/info "Validation GET response status:" (:status download-response) "body:" (:body download-response))
        (is (= 200 (:status download-response)) "Should successfully download via presigned GET URL")
        (is (= test-data (:body download-response)) "Downloaded content should match uploaded data"))

      (let [ctx {:aws test-aws-config}
            delete-object {:s3 {:bucket {:name test-bucket}
                                :object {:key test-key}}}
            delete-result (s3/delete-object ctx delete-object)]
        (log/info "Validation cleanup delete result:" delete-result)
        (is (not (:error delete-result)) "Should successfully delete validation test object")))))
