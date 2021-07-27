(ns aws
  (:require
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [lambda.request :as request]
   [sdk.aws.common :as common]
   [clojure.string :as str]
   [sdk.aws.cognito-idp :as cognito-idp]
   [sdk.aws.s3 :as s3]
   [sdk.aws.sqs :as sqs]))

(def put-object s3/put-object)
(def get-object s3/get-object)

(defn get-secret-value
  [secret]
  (let [req {:method     "POST"
             :uri        "/"
             :query      ""
             :payload    (str "{\"SecretId\" : \"" secret "\"}")
             :headers    {"X-Amz-Target" "secretsmanager.GetSecretValue"
                          "Host"         "secretsmanager.eu-central-1.amazonaws.com"
                          "Content-Type" "application/x-amz-json-1.1"
                          "X-Amz-Date"   (common/create-date)}
             :service    "secretsmanager"
             :region     "eu-central-1"
             :access-key (System/getenv "AWS_ACCESS_KEY_ID")
             :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
        auth (common/authorize req)]

    (let [response (common/retry
                    #(util/http-post
                      (str "https://" (get (:headers req) "Host"))
                      {:body    (:payload req)
                       :headers (-> (:headers req)
                                    (dissoc "Host")
                                    (assoc
                                     "X-Amz-Security-Token" (System/getenv "AWS_SESSION_TOKEN")
                                     "Authorization" auth))
                       :timeout 5000})
                    3)]
      (when (contains? response :error)
        (log/error "Failed to fetch secret" response))
      (:SecretString
       (:body response)))))

(def sqs-publish sqs/sqs-publish)

(defn sns-publish
  [topic message]
  (let [req {:method     "POST"
             :uri        "/"
             :payload    (str "Action=Publish"
                              "&TopicArn=arn:aws:sns:eu-central-1:"
                              (System/getenv "AccountId")
                              ":"
                              (System/getenv "EnvironmentNameLower")
                              "-"
                              topic
                              "&Message=" (util/url-encode message)
                              "&Expires=2020-10-15T12%3A00%3A00Z"
                              "&Version=2010-03-31&")
             :headers    {"Host"         "sns.eu-central-1.amazonaws.com"
                          "Content-Type" "application/x-www-form-urlencoded"
                          "Accept"       "application/json"
                          "X-Amz-Date"   (common/create-date)}
             :service    "sns"
             :region     "eu-central-1"
             :access-key (System/getenv "AWS_ACCESS_KEY_ID")
             :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
        auth (common/authorize req)]

    (let [response (common/retry #(util/http-post
                                   (str "https://"
                                        (get (:headers req) "Host")
                                        (:uri req))
                                   {:body    (:payload req)
                                    :headers (-> (:headers req)
                                                 (dissoc "Host")
                                                 (assoc
                                                  "Authorization" auth
                                                  "X-Amz-Security-Token" (System/getenv "AWS_SESSION_TOKEN")))
                                    :timeout 5000}) 3)]

      (when (contains? response :error)
        (log/error "Failed to sns:SendMessage" response))
      response)))

(defn get-next-request [runtime-api]
  (let [req (util/http-get
             (str "http://" runtime-api "/2018-06-01/runtime/invocation/next")
             {:timeout 90000000})
        body (:body req)]
    (log/debug "Lambda request" req)
    (if (:isBase64Encoded body)
      (assoc-in req
                [:body :body]
                (util/base64decode (:body body)))
      req)))

(def response
  {:statusCode 200
   :headers    {"Content-Type" "application/json"}})

(defn enqueue-response
  [ctx body]
  (let [resp (get @request/*request* :cache-keys)]
    (when resp
      (log/info "Distributing response")
      (let [{:keys [error]} (sqs/sqs-publish
                             (assoc ctx :queue "glms-router-svc-response"
                                    :message (util/to-json
                                              {:Records resp})))]
        (when error
          (throw (ex-info "Distribution failed" error)))))))

(defn send-success
  [{:keys [api
           invocation-id
           from-api] :as ctx} body]

  (log/info "Response" from-api)
  (util/d-time "Enqueueing success"
               (enqueue-response ctx body))
  (util/to-json
   (util/http-post
    (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/response")
    {:body (util/to-json body)})))

(defn delete-message-batch
  [ctx records]
  (log/info (first records))
  (let [queue-arn (get-in (first records) [:eventSourceARN])
        parts (str/split queue-arn #":")
        queue-name (peek parts)
        account-id (peek (pop parts))
        req {:method     "POST"
             :payload    (str "Action=DeleteMessageBatch"
                              "&QueueUrl=https://sqs.eu-central-1.amazonaws.com"
                              (str "/"
                                   account-id
                                   "/"
                                   queue-name)
                              (str/join (map-indexed
                                         (fn [idx %]
                                           (str "&DeleteMessageBatchRequestEntry." (inc idx) ".Id="
                                                (:messageId %)
                                                "&DeleteMessageBatchRequestEntry." (inc idx) ".ReceiptHandle="
                                                (:receiptHandle %)))
                                         records))
                              "&Expires=2020-04-18T22%3A52%3A43PST"
                              "&Version=2012-11-05")
             :uri        (str "/"
                              account-id
                              "/"
                              queue-name)
             :headers    {"Host"         "sqs.eu-central-1.amazonaws.com"
                          "Content-Type" "application/x-www-form-urlencoded"
                          "Accept"       "application/json"
                          "X-Amz-Date"   (common/create-date)}
             :service    "sqs"
             :region     "eu-central-1"
             :access-key (System/getenv "AWS_ACCESS_KEY_ID")
             :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
        auth (common/authorize req)]
    (log/info "Dispatching message delete")
    (let [response (common/retry
                    #(util/http-post
                      (str "https://"
                           (get (:headers req) "Host")
                           (:uri req))
                      {:body    (:payload req)
                       :version :http1.1
                       :headers (-> (:headers req)
                                    (dissoc "Host")
                                    (assoc "Authorization" auth)
                                    (assoc "X-Amz-Security-Token" (System/getenv "AWS_SESSION_TOKEN")))
                       :timeout 5000
                       :raw     true}) 3)]
      (when (or (contains? response :error)
                (> (get response :status 0) 299))
        (log/error "Failed to sqs:ChangeMessageVisibility" response))
      (:body response))))

(defn send-error
  [{:keys [api
           invocation-id
           from-api
           req] :as ctx} body]
  (let [resp (util/to-json body)
        target (if from-api
                 "response"
                 "error")]
    (when-not from-api
      (let [items (interleave body
                              (:Records req))
            items-to-delete (->> (partition 2 items)
                                 (filter (fn [[a _]]
                                           (not (:error a))))
                                 (map
                                  (fn [[_ b]]
                                    b)))]
        (when (and (> (count items-to-delete) 0)
                   (= (count body)
                      (count (:Records req))))
          (delete-message-batch ctx items-to-delete))))

    (util/d-time "Enqueueing error"
                 (enqueue-response ctx body))

    (log/error (util/to-json
                (util/http-post
                 (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/" target)
                 {:body resp})))))

(defn get-or-set
  [cache key get-fn]
  (let [current-time (util/get-current-time-ms)
        meta (get-in cache [:meta key])]
    (if (> (- current-time (get meta :time 0)) 1800000)
      (-> cache
          (assoc-in [:meta key] {:time current-time})
          (assoc key (common/retry
                      get-fn
                      2)))
      cache)))

(defn admin-auth
  [ctx]
  (let [{:keys [error] :as response} (cognito-idp/admin-initiate-auth ctx)]
    (if error
      response
      (get-in response [:AuthenticationResult :IdToken]))))

(defn get-token-from-cache
  [ctx]
  (let [{:keys [id-token]} (swap! util/*cache*
                                  (fn [cache]
                                    (get-or-set cache
                                                :id-token
                                                (fn []
                                                  (admin-auth ctx)))))]
    id-token))

(defn get-token
  [ctx]
  (if (:id-token ctx)
    (:id-token ctx)
    (get-token-from-cache ctx)))
