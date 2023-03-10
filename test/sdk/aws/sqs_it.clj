(ns sdk.aws.sqs-it
  (:require [sdk.aws.sqs :as sqs]
            [aws.ctx :as aws-ctx]
            [clojure.string :as string]
            [lambda.util :as util]
            [clojure.test :refer [deftest testing is]]))

; IMPORTANT IMPORTANT IMPORTANT
; Tests might leave some garbage in queues

(deftest test-send-and-receive
  (testing "Test simple single send and receive from it quque"
    (let [ctx (aws-ctx/init {:environment-name-lower (util/get-env "EnvironmentNameLower")})
          message "test-message"
          queue (str (util/get-env "AccountId")
                     "-"
                     (util/get-env "EnvironmentNameLower")
                     "-"
                     "it")]
      (is (= {:success true
              :id "id-1"}
             (sqs/sqs-publish (assoc ctx
                                     :queue queue
                                     :message {:id "id-1"
                                               :body message}))))
      (Thread/sleep 1000)
      (let [resp (sqs/sqs-receive (assoc ctx
                                         :queue queue))]
        (is (= [message]
               (mapv :body resp)))

        (is (= [1 0]
               (sqs/delete-message-batch (assoc ctx
                                                :queue queue
                                                :messages (conj resp
                                                                {:message-id "some"
                                                                 :receipt-handle "some"})))))))))

(deftest test-send-to-mossinf-queue
  (testing "Test simple single send missing queue"
    (let [ctx (aws-ctx/init {:environment-name-lower (util/get-env "EnvironmentNameLower")})
          message "test-message"
          queue (str (util/get-env "AccountId")
                     "-"
                     (util/get-env "EnvironmentNameLower")
                     "-"
                     "non-existing")]

      (try
        (sqs/sqs-publish (assoc ctx
                                :queue queue
                                :message {:id "id-1"
                                          :body message}))
        (throw (ex-info "Not to be here" {}))
        (catch Exception e
          (is (= "The specified queue does not exist for this wsdl version."
                 (get-in (ex-data e)
                         [:exception :Error :Message]))))))))

(deftest test-big-payload
  (testing "Test simple single send and receive from it quque"
    (let [ctx (aws-ctx/init {:environment-name-lower (util/get-env "EnvironmentNameLower")})
          message (string/join
                   ""
                   (range 0, 300000))
          queue (str (util/get-env "AccountId")
                     "-"
                     (util/get-env "EnvironmentNameLower")
                     "-"
                     "it")]
      (is (= {:success true
              :id "id-1"}
             (sqs/sqs-publish (assoc ctx
                                     :queue queue
                                     :message {:id "id-1"
                                               :body message}))))
      (Thread/sleep 1000)
      (let [resp (sqs/sqs-receive (assoc ctx
                                         :queue queue))]
        (is (= [{:name (str
                        (get-in ctx [:aws :account-id])
                        "-"
                        (get-in ctx [:environment-name-lower])
                        "-sqs")}]
               (mapv
                #(-> %
                     :body
                     util/to-edn
                     :s3
                     :bucket)
                resp)))

        (is (= [1 0]
               (sqs/delete-message-batch (assoc ctx
                                                :queue queue
                                                :messages (conj resp
                                                                {:message-id "some"
                                                                 :receipt-handle "some"})))))))))

(deftest test-batch
  (testing "Test sending batch"
    (let [ctx (aws-ctx/init {:environment-name-lower (util/get-env "EnvironmentNameLower")})
          messages [{:id "id1"
                     :body "msg-1"}
                    {:id "id2"
                     :body "msg-2"}]

          queue (str (util/get-env "AccountId")
                     "-"
                     (util/get-env "EnvironmentNameLower")
                     "-"
                     "it")]
      (is (= [{:success true
               :id "id1"}
              {:success true
               :id "id2"}]
             (sqs/sqs-publish-batch (assoc ctx
                                           :queue queue
                                           :messages messages))))
      (Thread/sleep 1000)
      (let [resp (sqs/sqs-receive (assoc ctx
                                         :queue queue
                                         :max-number-of-messages 10))]
        (is (= ["msg-1" "msg-2"]
               (mapv
                #(-> %
                     :body)
                resp)))

        (is (= [1 1]
               (sqs/delete-message-batch (assoc ctx
                                                :queue queue
                                                :messages resp))))))))

(deftest test-batch-missing-queu
  (testing "Test sending batch to non exsting queue"
    (let [ctx (aws-ctx/init {:environment-name-lower (util/get-env "EnvironmentNameLower")})
          messages [{:id "id1"
                     :body "msg-1"}
                    {:id "id2"
                     :body "msg-2"}]

          queue (str (util/get-env "AccountId")
                     "-"
                     (util/get-env "EnvironmentNameLower")
                     "-"
                     "non-existing")]

      (try
        (sqs/sqs-publish-batch (assoc ctx
                                      :queue queue
                                      :messages messages))
        (throw (ex-info "Not to be here" {}))
        (catch Exception e
          (is (= "The specified queue does not exist for this wsdl version."
                 (get-in (ex-data e)
                         [:exception :Error :Message]))))))))

