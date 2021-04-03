(ns sdk.aws.cognito-idp
  (:require [lambda.util :as util]
            [sdk.aws.common :as common]
            [clojure.tools.logging :as log]))

(defn- cognito-request
  [{:keys
    [command
     aws-access-key-id
     aws-secret-access-key
     aws-session-token]} payload]
  (let [req {:method     "POST"
             :uri        "/"
             :query      ""
             :payload    (util/to-json payload)
             :headers    {"X-Amz-Target" (str "AWSCognitoIdentityProviderService."
                                              command)
                          "Host"         "cognito-idp.eu-central-1.amazonaws.com"
                          "Content-Type" "application/x-amz-json-1.1"
                          "X-Amz-Date"   (common/create-date)}
             :service    "cognito-idp"
             :region     "eu-central-1"
             :access-key aws-access-key-id
             :secret-key aws-secret-access-key}
        auth (common/authorize req)]

    (let [response (common/retry
                    #(util/http-post
                      (str "https://" (get (:headers req) "Host"))
                      {:body    (:payload req)
                       :headers (-> (:headers req)
                                    (dissoc "Host")
                                    (assoc
                                     "X-Amz-Security-Token" aws-session-token
                                     "Authorization" auth))
                       :timeout 5000})
                    3)]
      (log/debug "Auth response" response)
      (cond
        (contains? response :error) (do
                                      (log/error "Failed update" response)
                                      {:error (:error response)})
        (> (:status response) 299) (do
                                     (log/error "Auth failure response"
                                                (:status response)
                                                (:body response))
                                     {:error {:status (:status response)}})
        :else response))))

(defn admin-initiate-auth
  [{aws                                     :aws
    {username :username password :password} :svc
    {client-id     :client-id
     client-secret :client-secret
     user-pool-id  :user-pool-id}           :auth}]

  (let [{:keys [error] :as response}
        (cognito-request
         (assoc aws
                :command "AdminInitiateAuth")
         {:AuthFlow       "ADMIN_NO_SRP_AUTH"
          :AuthParameters {:USERNAME    username
                           :PASSWORD    password
                           :SECRET_HASH (util/hmac-sha256
                                         client-secret
                                         (str username client-id))}
          :ClientId       client-id
          :UserPoolId     user-pool-id})]
    (if error
      response
      (get-in response [:body]))))

(defn admin-get-user
  [{{user-pool-id :user-pool-id} :auth
    aws                          :aws} {:keys [username]}]

  (let [{:keys [error] :as response}
        (cognito-request
         (assoc aws
                :command "AdminGetUser")
         {:Username   username
          :UserPoolId user-pool-id})]
    (if error
      response
      (get-in response [:body]))))

(defn admin-list-groups-for-user
  [{{user-pool-id :user-pool-id} :auth
    aws                          :aws} {:keys [username]}]

  (let [{:keys [error] :as response}
        (cognito-request
         (assoc aws
                :command "AdminListGroupsForUser")
         {:Username   username
          :UserPoolId user-pool-id
          :Limit      60})]
    (if error
      response
      (get-in response [:body]))))

