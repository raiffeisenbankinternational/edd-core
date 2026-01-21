(ns aws.athena-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [aws.athena :as athena]
   [clojure.test :refer [deftest is testing]]))

(defn resource->edn [filename]
  (-> (format "resources/athena/%s" filename)
      io/resource
      slurp
      edn/read-string))

(defn resource->fn [filename]
  (-> filename
      resource->edn
      constantly))

(deftest test-execution->bucket

  (let [{:keys [QueryExecution]}
        (resource->edn "GetQueryExecution.edn")

        bucket
        (athena/execution->bucket QueryExecution)]

    (is (= "118123141711-dev19-sqs" bucket))))

(deftest test-execution->key-path

  (let [{:keys [QueryExecution]}
        (resource->edn "GetQueryExecution.edn")

        key-path
        (athena/execution->key-path QueryExecution)]

    (is (= "158e17c1-ef19-46d7-bd54-65f859868711.csv"
           key-path))))

(deftest test-execution->s3-object

  (let [{:keys [QueryExecution]}
        (resource->edn "GetQueryExecution.edn")

        s3-object
        (athena/execution->s3-object QueryExecution)]

    (is (= {:s3
            {:bucket {:name "118123141711-dev19-sqs"}
             :object {:key "158e17c1-ef19-46d7-bd54-65f859868711.csv"}}}
           s3-object))))

(deftest test-execution->submission-epoch

  (let [{:keys [QueryExecution]}
        (resource->edn "GetQueryExecution.edn")

        epoch
        (athena/execution->submission-epoch QueryExecution)]

    (is (= 1704979253
           (long epoch)))))

(deftest test-find-last-execution

  (let [[result1
         result2
         result3]
        (with-redefs [athena/list-query-executions
                      (resource->fn "ListQueryExecutions.edn")

                      athena/batch-get-query-execution
                      (resource->fn "BatchGetQueryExecution.edn")]

          [(athena/find-last-execution nil
                                       "test_limit_review_db"
                                       "118123141711-dev19-sqs"
                                       "select aggregate from test_dimension"
                                       (long 1.704914264467E9))

           (athena/find-last-execution nil
                                       "missing_db"
                                       "118123141711-dev19-sqs"
                                       "select aggregate from test_dimension"
                                       (long 1.704914264467E9))

           (athena/find-last-execution nil
                                       "test_limit_review_db"
                                       "118123141711-dev19-sqs"
                                       "select aggregate from test_dimension"
                                       (long (+ 1.704914264467E9 100000)))])]

    (is (some? result1))
    (is (nil? result2))
    (is (nil? result3))))

(deftest test-poll-query-execution-ok
  (let [result
        (with-redefs [athena/get-query-execution
                      (resource->fn "GetQueryExecution.edn")]
          (athena/poll-query-execution nil "123"))]
    (is (some? result))))

(deftest test-poll-query-execution-thrown
  (with-redefs [athena/get-query-execution
                (resource->fn "GetQueryExecution_Error.edn")]
    (try
      (athena/poll-query-execution nil "123")
      (is false)
      (catch Throwable e
        (is true)
        (is (= "Athena execution has been terminated, id: 123, state: FAILED, message: TABLE_NOT_FOUND: line 1:23: Table 'awsdatacatalog.test_limit_review_db.test_applicationaaaa' does not exist"
               (ex-message e)))))))

(def S3-URL-WITH-SPACE
  "s3://055194627518-foo-athena-query/Matched Exposure/2024/11/12/xxx.csv")

(deftest test-execution->bucket
  (let [data
        {:ResultConfiguration
         {:OutputLocation S3-URL-WITH-SPACE}}]
    (is (= "055194627518-foo-athena-query"
           (athena/execution->bucket data)))))

(deftest test-execution->key-path
  (let [data
        {:ResultConfiguration
         {:OutputLocation S3-URL-WITH-SPACE}}]
    (is (= "Matched Exposure/2024/11/12/xxx.csv"
           (athena/execution->key-path data)))))
