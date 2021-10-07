(ns sdk.aws.sqs
  (:require [lambda.util :as util]
            [sdk.aws.common :as sdk]
            [lambda.http-client :as client]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (java.net URLEncoder)))

(def batch-version "2012-11-05")

(def retry-count 3)

(defn sqs-publish
  [{:keys [queue ^String message aws] :as ctx}]
  (let [req {:method "POST"
             :uri (str "/"
                       (:account-id aws)
                       "/"
                       (:environment-name-lower ctx)
                       "-"
                       queue)
             :query ""
             :payload (str "Action=SendMessage"
                           "&MessageBody=" (URLEncoder/encode message "UTF-8"))
             :headers {"Host" (str "sqs."
                                   (get aws :region)
                                   ".amazonaws.com")
                       "Content-Type" "application/x-www-form-urlencoded"
                       "Accept" "application/json"
                       "X-Amz-Date" (sdk/create-date)}
             :service "sqs"
             :region (get aws :region)
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        auth (sdk/authorize req)]

    (let [response (client/retry-n
                    #(util/http-post
                      (str "https://"
                           (get (:headers req) "Host")
                           (:uri req))
                      (client/request->with-timeouts
                       %
                       {:body (:payload req)
                        :version :http1.1
                        :headers (-> (:headers req)
                                     (assoc "Authorization" auth)
                                     (dissoc "Host")
                                     (assoc "X-Amz-Security-Token"
                                            (:aws-session-token aws)))}))
                    :retries retry-count)]
      (when (contains? response :error)
        (throw (-> "Failed to send message" (ex-info response))))

      (if (> (:status response) 299)
        {:error {:queue (:uri req)
                 :body (:body response)
                 :message (get-in response [:body :Error :Message])}}
        (:body response)))))

(defn delete-message-batch
  [{:keys [aws] :as ctx} records]
  (log/info (first records))
  (let [queue-arn (get-in (first records) [:eventSourceARN])
        parts (str/split queue-arn #":")
        queue-name (peek parts)
        account-id (peek (pop parts))
        req {:method "POST"
             :payload (str "Action=DeleteMessageBatch"
                           "&QueueUrl=https://sqs."
                           (get aws :region)
                           ".amazonaws.com"
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
             :uri (str "/"
                       account-id
                       "/"
                       queue-name)
             :headers {"Host" (str "sqs."
                                   (:region aws)
                                   ".amazonaws.com")
                       "Content-Type" "application/x-www-form-urlencoded"
                       "Accept" "application/json"
                       "X-Amz-Date" (sdk/create-date)}
             :service "sqs"
             :region (get aws :region)
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        auth (sdk/authorize req)]
    (log/info "Dispatching message delete")
    (let [response (client/retry-n
                    #(util/http-post
                      (str "https://"
                           (get (:headers req) "Host")
                           (:uri req))
                      (client/request->with-timeouts
                       %
                       {:body (:payload req)
                        :version :http1.1
                        :headers (-> (:headers req)
                                     (dissoc "Host")
                                     (assoc "Authorization" auth)
                                     (assoc "X-Amz-Security-Token" (:aws-session-token aws)))
                        :raw true}))
                    :retries retry-count)]
      (when (or (contains? response :error)
                (> (get response :status 0) 299))
        (log/error "Failed to sqs:ChangeMessageVisibility" response))
      (:body response))))