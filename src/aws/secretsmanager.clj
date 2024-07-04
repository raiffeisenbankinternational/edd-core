(ns aws.secretsmanager
  (:require
   [clojure.tools.logging :as log]
   [lambda.util :as util]
   [sdk.aws.common :as common]))

(def ^:const TIMEOUT 5000)
(def ^:const RETRIES 3)

(defn make-request-no-retry
  "
  The common request function to AWS Secrets Manager with no retry logic.
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
        (format "secretsmanager.%s.amazonaws.com" region)

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
         :service    "secretsmanager"
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

        _
        (log/infof "AWS SecretsManager request, url: %s, action: %s, body: %s"
                   url action body)

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
            (format "AWS Secrets Manager error, action: %s, status: %s, type: %s, url: %s, message: %s"
                    action status __type url final-message)]
        (throw (ex-info report body))))))

(defn make-request
  "
  Like `make-request-no-retry` but driven with retry-on-exception logic.
  "
  [aws ^String action data]
  (common/with-retry [RETRIES]
    (make-request-no-retry aws action data)))

(defn get-secret-value
  "
  https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html

  Return a map where the :SecretString key
  holds the string payload of the secret.
  "

  ([aws secret-id]
   (get-secret-value aws secret-id nil))

  ([aws secret-id {:keys [version-id
                          version-stage]}]

   (let [action
         "secretsmanager.GetSecretValue"

         data
         (cond-> {:SecretId secret-id}

           version-id
           (assoc :VersionId version-id)

           version-stage
           (assoc :VersionStage version-stage))]

     (make-request-no-retry aws action data))))

(defn list-secret-values
  "
  https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_ListSecrets.html
  "
  ([aws]
   (list-secret-values aws nil))

  ([aws {:keys [max-results
                next-token
                sort-order]}]

   (let [action
         "secretsmanager.ListSecrets"

         data
         (cond-> {}

           max-results
           (assoc :MaxResults max-results)

           next-token
           (assoc :NextToken next-token)

           sort-order
           (assoc :SortOrder sort-order))]

     (make-request-no-retry aws action data))))
