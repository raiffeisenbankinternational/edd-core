(ns aws.dynamodb
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [sdk.aws.common :as common]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- extract-table-names
  "Extract all table names from DynamoDB request body"
  [body]
  (let [direct-table (when-let [t (:TableName body)] [t])
        transact-tables (when-let [items (:TransactItems body)]
                          (->> items
                               (keep (fn [item]
                                       (or (get-in item [:Put :TableName])
                                           (get-in item [:Update :TableName])
                                           (get-in item [:Delete :TableName])
                                           (get-in item [:ConditionCheck :TableName]))))
                               (distinct)))]
    (distinct (concat direct-table transact-tables))))

(defn make-request
  [{:keys [aws action body]}]
  (log/info "Make request" body)
  (let [table-names (extract-table-names body)
        req {:method     "POST"
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
                    3)
          status (long (:status response))]
      (when (contains? response :error)
        (throw (ex-info "Invocation error" (:error response))))
      (when (> status 399)
        (let [error-body (:body response)
              is-not-found? (and (map? error-body)
                                 (= (:__type error-body) "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"))
              error-msg (if (and is-not-found? (seq table-names))
                          (str "Invocation error (table(s): " (string/join ", " table-names) ")")
                          "Invocation error")]
          (when (and is-not-found? (seq table-names))
            (log/error "DynamoDB table(s) not found:" (string/join ", " table-names)))
          (throw (ex-info error-msg error-body))))
      (:body response))))

(defn list-tables
  [ctx]
  (make-request
   (assoc ctx :action "ListTables"
          :body {})))
