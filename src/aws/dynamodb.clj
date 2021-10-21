(ns aws.dynamodb
  (:require [clojure.tools.logging :as log]
            [lambda.util :as util]
            [sdk.aws.common :as common]))

(defn make-request
  [{:keys [aws action body]}]
  (let [req {:method     "POST"
             :uri        "/"
             :query      ""
             :payload    (util/to-json body)
             :headers    {"X-Amz-Target"         (str "DynamoDB_20120810." action)
                          "Host"                 (str "dynamodb." (:region aws) ".amazonaws.com")
                          "Content-Type"         "application/x-amz-json-1.0"
                          "X-Amz-Security-Token" (:aws-session-token aws)
                          "X-Amz-Date"           (common/create-date)}
             :service    "dynamodb"
             :region     (:region aws)
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        auth (common/authorize req)]

    (let [response (common/retry
                    #(util/http-post
                      (str "https://" (get (:headers req) "Host") "/")
                      {:body    (:payload req)
                       :headers (-> (:headers req)
                                    (dissoc "Host")
                                    (assoc "Authorization" auth))
                       :timeout 5000})
                    3)]
      (when (contains? response :error)
        (log/error "Failed to fetch secret" response))
      (:body response))))

(defn list-tables
  [ctx]
  (make-request
   (assoc ctx :action "ListTables"
          :body {})))