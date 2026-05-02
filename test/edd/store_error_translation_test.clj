(ns edd.store-error-translation-test
  "Unit tests for the store error contract: a failed unique/conditional write is
   translated into a user-friendly key, distinguishing a permanent identity
   collision (:identity-conflict) from a retryable event-seq clash
   (:concurrent-modification). Both the postgres and dynamodb stores must agree."
  (:require [clojure.test :refer [deftest is testing]]
            [edd.postgres.event-store :as pg]
            [edd.dynamodb.event-store :as ddb]))

(deftest postgres-error-translation
  (testing "identity_store unique violation -> :identity-conflict"
    (is
     (= :identity-conflict
        (:key (pg/parse-error
               "ERROR: duplicate key value violates unique constraint \"identity_store_pkey\"")))))

  (testing "event_store unique violation -> :concurrent-modification"
    (is
     (= :concurrent-modification
        (:key (pg/parse-error
               "ERROR: duplicate key value violates unique constraint \"event_store_pkey\"")))))

  (testing "the original message is preserved"
    (is
     (= "ERROR: duplicate key value violates unique constraint \"identity_store_pkey\""
        (:original-message (pg/parse-error
                            "ERROR: duplicate key value violates unique constraint \"identity_store_pkey\"")))))

  (testing "an unrelated error is returned unchanged (no :key)"
    (is
     (= "some unrelated failure"
        (pg/parse-error "some unrelated failure")))))

(deftest dynamodb-conditional-failure-detection
  (testing "TransactionCanceledException is recognised as a conditional failure"
    (is
     (true?
      (#'ddb/conditional-failure?
       {:__type "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
        :CancellationReasons [{:Code "None"} {:Code "ConditionalCheckFailed"}]}))))

  (testing "a non-conditional error is not treated as a conflict"
    (is
     (false?
      (#'ddb/conditional-failure?
       {:__type "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"})))))

(deftest dynamodb-conflict-classification
  (testing "a failed identity item -> :identity-conflict"
    (is
     (= :identity-conflict
        (ddb/conflict-key
         {:CancellationReasons [{:Code "None"} {:Code "None"} {:Code "ConditionalCheckFailed"}]}
         [:event :effect :identity]))))

  (testing "a failed event item -> :concurrent-modification"
    (is
     (= :concurrent-modification
        (ddb/conflict-key
         {:CancellationReasons [{:Code "ConditionalCheckFailed"} {:Code "None"}]}
         [:event :identity]))))

  (testing "no parseable failed reason falls back to :concurrent-modification"
    (is
     (= :concurrent-modification
        (ddb/conflict-key {:CancellationReasons []} [:event :identity])))))
