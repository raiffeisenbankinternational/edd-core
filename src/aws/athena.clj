(ns aws.athena
  "
  Everything we need to become friends with AWS Athena.
  https://docs.aws.amazon.com/athena/latest/APIReference/API_Operations.html
  See the data samples captured on dev19 in test/resources/athena/ directory.
  "
  (:import java.net.URL)
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [sdk.aws.common :as common]))

(def ^:const TIMEOUT 5000)
(def ^:const RETRIES 3)

(defn make-request-no-retry
  "
  The common request function to Athena with no retry logic.
  Either return a parsed JSON or throw an exception when
  the HTTP status code was not 2xx.
  "
  [aws ^String action data]

  (let [{:keys [region
                aws-session-token
                aws-access-key-id
                aws-secret-access-key]}
        aws

        host
        (format "athena.%s.amazonaws.com" region)

        headers-sign
        {"host"                 host
         "Content-Type"         "application/x-amz-json-1.1"
         "x-amz-date"           (common/create-date)
         "x-amz-security-token" aws-session-token
         "x-amz-target"         action}

        body
        (util/to-json data)

        map-to-sign
        {:headers    headers-sign
         :method     "POST"
         :uri        "/"
         :query      ""
         :service    "athena"
         :region     region
         :access-key aws-access-key-id
         :secret-key aws-secret-access-key
         :payload    body}

        auth
        (common/authorize map-to-sign)

        url
        (str "https://" host)

        headers-request
        (-> headers-sign
            (dissoc "host")
            (assoc "Authorization" auth))

        request
        {:body body
         :headers headers-request
         :timeout TIMEOUT}

        response
        (util/http-post url request)

        {:keys [status body]}
        response

        {:keys [Message
                message
                __type]}
        body

        final-message
        (or Message message)

        ok?
        (<= 200 status 299)]

    (if ok?
      body
      (let [report
            (format "Athena error, action: %s, status: %s, type: %s, url: %s, message: %s"
                    action status __type url final-message)]
        (throw (ex-info report body))))))

(defn make-request
  "
  Like `make-request-no-retry` but driven with retry-on-exception logic.
  "
  [aws ^String action data]
  (common/with-retry [RETRIES]
    (make-request-no-retry aws action data)))

(defn list-query-executions
  "
  https://docs.aws.amazon.com/athena/latest/APIReference/API_ListQueryExecutions.html
  "
  ([aws]
   (list-query-executions aws nil))

  ([aws {:keys [max-results
                next-token
                work-group]}]

   (let [action
         "AmazonAthena.ListQueryExecutions"

         data
         (cond-> {}

           max-results
           (assoc :MaxResults max-results)

           next-token
           (assoc :NextToken next-token)

           work-group
           (assoc :WorkGroup work-group))]

     (make-request aws action data))))

(defn start-query-execution
  "
  https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html
  "
  ([aws
    ^String database
    ^String bucket
    ^String query]
   (start-query-execution aws
                          database
                          bucket
                          query
                          nil))

  ([aws
    ^String database
    ^String bucket
    ^String query
    {:keys [work-group
            catalog]
     :or {work-group "primary"
          catalog "AwsDataCatalog"}}]

   (let [request-token
         (str (uuid/gen))

         output-location
         (format "s3://%s" bucket)

         data
         {:ClientRequestToken request-token
          :QueryString query
          :QueryExecutionContext
          {:Database database
           :Catalog catalog}
          :WorkGroup work-group
          :ResultConfiguration
          {:OutputLocation output-location}}

         action
         "AmazonAthena.StartQueryExecution"]

     (make-request aws action data))))

(defn batch-get-query-execution
  "
  https://docs.aws.amazon.com/athena/latest/APIReference/API_BatchGetQueryExecution.html
  "
  [aws execution-ids]
  (let [action
        "AmazonAthena.BatchGetQueryExecution"
        data
        {:QueryExecutionIds execution-ids}]
    (make-request aws action data)))

(defn get-query-execution
  "
  https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html
  "
  [aws execution-id]
  (let [action
        "AmazonAthena.GetQueryExecution"
        data
        {:QueryExecutionId execution-id}]
    (make-request aws action data)))

(defn execution->state ^String [QueryExecution]
  (some-> QueryExecution
          :Status
          :State))

(defn execution->submission-epoch
  ^double [QueryExecution]
  (some-> QueryExecution
          :Status
          :SubmissionDateTime))

(defn execution->error ^String [QueryExecution]
  (some-> QueryExecution
          :Status
          :AthenaError
          :ErrorMessage))

(defn ->URL
  "
  Get an instance of `java.net.URL` from a string like:
  s3://some.bucket/path/to/some special/file.txt
                              ^^^
  Pay attention to the space above. With a space, the
  `java.net.URI` class fails parsing it. The `java.net.URL`
  class feels better but it accepts only `http(s)://`
  protocol.

  Thus, change the protocol first, then parse.
  "
  ^URL [^String s3-url]
  (-> s3-url
      (str/replace #"^s3://" "https://")
      (java.net.URL.)))

(defn execution->bucket ^String [QueryExecution]
  (some-> QueryExecution
          :ResultConfiguration
          :OutputLocation
          ->URL
          .getHost))

(defn execution->key-path ^String [QueryExecution]
  (when-let [key-path
             (some-> QueryExecution
                     :ResultConfiguration
                     :OutputLocation
                     ->URL
                     .getPath)]
    (if (str/starts-with? key-path "/")
      (subs key-path 1)
      key-path)))

(defn execution->s3-object ^String [QueryExecution]
  (let [bucket
        (execution->bucket QueryExecution)

        key-path
        (execution->key-path QueryExecution)]

    {:s3 {:bucket {:name bucket}
          :object {:key key-path}}}))

(defn poll-query-execution
  "
  Like get-query-execution but waits until the status of the result
  is SUCCEEDED. Retry if the query is still in processing or throw
  an exception if a negative status met. Also throw an error when
  draining all the attempts.
  "
  ([aws execution-id]
   (poll-query-execution aws execution-id nil))

  ([aws execution-id {:keys [retries timeout]
                      :or {retries 30 timeout 15000}}]

   (loop [n retries]

     (when (zero? n)
       (throw (ex-info "Retry attempts have been exhaused"
                       {:execution-id execution-id
                        :retries retries
                        :timeout timeout})))

     (let [{:as result :keys [QueryExecution]}
           (get-query-execution aws execution-id)

           state
           (execution->state QueryExecution)]

       (case state

         ("SUCCEEDED")
         result

         ("RUNNING" "QUEUED")
         (do
           (Thread/sleep ^long timeout)
           (recur (dec n)))

         ("FAILED" "CANCELLED")
         (let [error
               (execution->error QueryExecution)

               message
               (format "Athena execution has been terminated, id: %s, state: %s, message: %s"
                       execution-id state error)]
           (throw (ex-info message
                           {:execution-id execution-id
                            :state state}))))))))

(defn find-last-execution
  "
  Having the input criteria, find the last QueryExecution node
  that matches them. The purpose of this function is to reuse
  the previous query instead of firing a new one (we're facing
  AWS rate limits).

  The epoch fields store seconds, not milliseconds.
  "
  [aws
   ^String database
   ^String bucket
   ^String query
   ^Long min-submission-epoch]

  (let [{:keys [QueryExecutionIds]}
        (list-query-executions aws {:max-results 50})

        {:keys [QueryExecutions]}
        (when (seq QueryExecutionIds)
          (batch-get-query-execution aws QueryExecutionIds))

        QueryExecutions
        (sort-by (comp - execution->submission-epoch)
                 QueryExecutions)

        pred
        (fn [QueryExecution]

          (let [{:keys [Query
                        QueryExecutionContext]}
                QueryExecution

                state
                (execution->state QueryExecution)

                {:keys [Database]}
                QueryExecutionContext

                submission-epoch
                (execution->submission-epoch QueryExecution)

                Bucket
                (execution->bucket QueryExecution)]

            (when (and (= state "SUCCEEDED")
                       (= Query query)
                       (= Database database)
                       (= Bucket bucket)
                       (<= min-submission-epoch submission-epoch))
              QueryExecution)))]

    (some pred QueryExecutions)))
