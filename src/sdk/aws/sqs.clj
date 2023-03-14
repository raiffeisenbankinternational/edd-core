(ns sdk.aws.sqs
  (:require [lambda.util :as util]
            [sdk.aws.s3 :as s3]
            [malli.core :as m]
            [clj-aws-sign.core :as aws-clj-sign-core]
            [lambda.uuid :as uuid]
            [sdk.aws.common :as sdk]
            [lambda.http-client :as client]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (java.net URLEncoder)))

(def MessageStandardSchema
  (m/schema
   [:map
    [:id [:string {:min 1}]]
    [:message [:string {:min 1}]]]))

(def MessageFifoSchema
  (m/schema
   [:map
    [:id [:string {:min 1}]]
    [:message [:string {:min 1}]]
    [:group-id [:string {:min 1}]]
    [:deduplication-id [:string {:min 1}]]]))

(def MessageStandardValidator
  (m/validator MessageStandardSchema))

(def MessageFifoValidator
  (m/validator MessageFifoSchema))

(def batch-version "2012-11-05")

(defn- convert-query-params
  [query]
  (->> query
       (sort (fn [[k1 _v1] [k2 _v2]] (compare k1 k2)))
       (map
        #(map (fn [m]
                (URLEncoder/encode m "UTF-8"))
              %))
       (#(map (fn [pair] (str/join "=" pair)) %))
       (str/join "&")))

(defn request-entity
  [index name value]
  (str "&SendMessageBatchRequestEntry." index "." name "=" value))

(defn message->query
  "Convert message to query parameters"
  [index {:keys [id group-id ^String body deduplication-id]}]
  (let [i (inc index)
        entity (partial request-entity i)]
    (when-not body
      (throw (ex-info "Message is required"
                      {:message body
                       :id id
                       :group-id group-id})))
    (str
     (entity "Id" id)
     (entity "MessageBody" (URLEncoder/encode body "UTF-8"))
     (when deduplication-id
       (entity "MessageDeduplicationId" deduplication-id))
     (when group-id
       (entity "MessageGroupId" group-id)))))

(defn response->result
  [resp]
  (let [{:keys [Failed Successful]}
        (get-in resp [:SendMessageBatchResponse
                      :SendMessageBatchResult])
        f (map (fn [{:keys [Id Message]}]
                 {:id Id :error Message :success false}) Failed)
        s (map (fn [{:keys [Id]}]
                 {:id Id :success true}) Successful)]
    (concat s f)))

(defn message-to-s3
  [{:keys [aws] :as ctx} message]
  (let [key (str "sqs-content/" (uuid/gen))
        object {:s3 {:bucket
                     {:name
                      (str
                       (get aws :account-id)
                       "-"
                       (get ctx :environment-name-lower)
                       "-sqs")}
                     :object {:key key}}}
        {:keys [error]} (s3/put-object ctx (assoc-in
                                            object
                                            [:s3 :object :content]
                                            message))]
    (when error
      (throw (ex-info "Unable to store SQS payload"
                      error)))
    (URLEncoder/encode (-> object
                           util/to-json)
                       "UTF-8")))

(defn is-message-too-big?
  [message]
  (< (-> message
         (.getBytes)
         alength)
     226214))

(defn sqs-publish
  [{:keys [queue message aws] :as ctx}]
  (let [message-id (if (string? message)
                     (uuid/gen)
                     (:id message))
        ^String message (if (string? message)
                          message
                          (:body message))

        message (URLEncoder/encode message "UTF-8")
        message (if (is-message-too-big? message)
                  message
                  (message-to-s3 ctx message))
        req {:method "POST"
             :uri (str "/"
                       (:account-id aws)
                       "/"
                       queue)
             :query ""
             :payload (str "Action=SendMessage"
                           "&MessageBody=" message)
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
        auth (sdk/authorize req)
        {:keys [status]
         :as response} (client/retry-n
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
                                                (:aws-session-token aws)))})))]
    (when (> status 399)
      (throw (ex-info "Failed to send message"
                      {:success false
                       :id message-id
                       :queue (:uri req)
                       :message (get-in response [:body :Error :Message])
                       :exception (:body response)})))

    {:success true
     :id message-id}))

(defn sqs-publish-batch
  [{:keys [queue messages aws] :as ctx}]
  (let [req {:method "POST"
             :uri (str "/"
                       (:account-id aws)
                       "/"
                       queue)
             :query ""
             :payload (str "Action=SendMessageBatch"
                           (->> messages
                                (map-indexed message->query)
                                (str/join))
                           "&Version="
                           batch-version)
             :headers {"Host" (str
                               "sqs."
                               (:region aws)
                               ".amazonaws.com")
                       "Content-Type" "application/x-www-form-urlencoded"
                       "Accept" "application/json"
                       "X-Amz-Date" (sdk/create-date)}
             :service "sqs"
             :region (:region aws)
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        auth (sdk/authorize req)
        response (client/retry-n
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
                  :retries 3)]
    (log/debug req)
    (cond
      (contains? response :error) (throw (ex-info "Failed to send message"
                                                  {:error response}))
      (> (:status response) 399) (throw (ex-info "Failed to send message"
                                                 {:queue (:uri req)
                                                  :exception (:body response)
                                                  :message (get-in response [:body :Error :Message])}))
      :else (-> response :body response->result))))

(defn delete-message-batch
  [{:keys [aws queue messages]}]
  (util/d-time
   (str "delete-messages-batch: " queue)
   (let [queue-url (str "https://sqs."
                        (get aws :region)
                        ".amazonaws.com"
                        "/"
                        (:account-id aws)
                        "/"
                        queue)
         req {:method "POST"
              :payload (str "Action=DeleteMessageBatch"
                            "&QueueUrl=" queue-url
                            (str/join (map-indexed
                                       (fn [idx %]
                                         (str "&DeleteMessageBatchRequestEntry." (inc idx) ".Id="
                                              (get % :message-id
                                                   (get % :messageId))
                                              "&DeleteMessageBatchRequestEntry." (inc idx) ".ReceiptHandle="
                                              (URLEncoder/encode (get % :receipt-handle
                                                                      (get % :receiptHandle)) "UTF-8")))
                                       messages))
                            "&Expires=2020-04-18T22%3A52%3A43PST"
                            "&Version=2012-11-05")
              :uri (str "/"
                        (:account-id aws)
                        "/"
                        queue)
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
                         :raw true})))
           response (->> response
                         :body
                         :DeleteMessageBatchResponse
                         :DeleteMessageBatchResult
                         :Failed
                         (reduce
                          (fn [p v]
                            (assoc p (:Id v) 0))
                          {}))]
       (mapv
        (fn [{:keys [message-id]}]
          (get response message-id 1))
        messages)))))

(defn sqs-receive
  [{:keys [queue aws max-number-of-messages]
    :or {max-number-of-messages 1}
    :as _ctx}]

  (util/d-time
   (str "sqs-receive: " queue)
   (with-redefs [aws-clj-sign-core/query->string convert-query-params]
     (let [query [["MaxNumberOfMessages" (str max-number-of-messages)]
                  ["Action" "ReceiveMessage"]]
           req {:method "GET"
                :uri (str "/"
                          (:account-id aws)
                          "/"
                          queue)
                :query query
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
           auth (sdk/authorize req)
           {:keys [status]
            :as response} (client/retry-n
                           #(util/http-get
                             (str "https://"
                                  (get (:headers req) "Host")
                                  (:uri req)
                                  "?"
                                  (convert-query-params query))
                             (client/request->with-timeouts
                              %
                              {:version :http1.1
                               :headers (-> (:headers req)
                                            (assoc "Authorization" auth)
                                            (dissoc "Host")
                                            (assoc "X-Amz-Security-Token"
                                                   (:aws-session-token aws)))})))]
       (when (> status 200)
         (throw (-> "Failed to receive message" (ex-info response))))

       (->> response
            :body
            :ReceiveMessageResponse
            :ReceiveMessageResult
            :messages
            (mapv
             (fn [m]
               {:body (:Body m)
                :message-id (:MessageId m)
                :receipt-handle (:ReceiptHandle m)})))))))

(defn sqs-purge
  [{:keys [queue aws] :as ctx}]
  (util/d-time
   (str "sqs-receive: " queue)
   (let [req {:method "GET"
              :uri (str "/"
                        (:account-id aws)
                        "/"
                        queue)
              :query [["Action" "PurgeQueue"]]
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
         auth (sdk/authorize req)
         {:keys [status]
          :as response} (client/retry-n
                         #(util/http-get
                           (str "https://"
                                (get (:headers req) "Host")
                                (:uri req))
                           (client/request->with-timeouts
                            %
                            {:version :http1.1
                             :query-params (convert-query-params (:query req))
                             :headers (-> (:headers req)
                                          (assoc "Authorization" auth)
                                          (dissoc "Host")
                                          (assoc "X-Amz-Security-Token"
                                                 (:aws-session-token aws)))})))]
     (when (> status 200)
       (throw (-> "Failed to purge message" (ex-info response))))
     response)))
