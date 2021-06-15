(ns sdk.aws.sqs
  (:require [lambda.util :as util]
            [sdk.aws.common :as sdk]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (java.net URLEncoder)))

(def batch-version "2012-11-05")

(defn sqs-publish
  [{:keys [queue ^String message aws] :as ctx}]
  (let [req {:method     "POST"
             :uri        (str "/"
                              (:account-id aws)
                              "/"
                              (:environment-name-lower ctx)
                              "-"
                              queue)
             :query      ""
             :payload    (str "Action=SendMessage"
                              "&MessageBody=" (URLEncoder/encode message "UTF-8"))
             :headers    {"Host"         (str "sqs."
                                              (get aws :region)
                                              ".amazonaws.com")
                          "Content-Type" "application/x-www-form-urlencoded"
                          "Accept"       "application/json"
                          "X-Amz-Date"   (sdk/create-date)}
             :service    "sqs"
             :region     "eu-central-1"
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        auth (sdk/authorize req)]

    (let [response (sdk/retry
                    #(util/http-post
                      (str "https://"
                           (get (:headers req) "Host")
                           (:uri req))
                      {:body    (:payload req)
                       :version :http1.1
                       :headers (-> (:headers req)
                                    (assoc "Authorization" auth)
                                    (dissoc "Host")
                                    (assoc "X-Amz-Security-Token"
                                           (:aws-session-token aws)))
                       :timeout 5000}) 3)]
      (when (contains? response :error)
        (throw (-> "Failed to send message" (ex-info response))))

      (if (> (:status response) 299)
        {:error {:queue   (:uri req)
                 :body    (:body response)
                 :message (get-in response [:body :Error :Message])}}
        (:body response)))))



