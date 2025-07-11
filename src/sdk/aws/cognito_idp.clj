(ns sdk.aws.cognito-idp
  (:require [lambda.util :as util]
            [sdk.aws.common :as common]
            [clojure.tools.logging :as log]
            [lambda.http-client :as client]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- cognito-request
  [{{:keys
     [aws-access-key-id
      aws-secret-access-key
      aws-session-token
      region]}
    :aws}
   command
   payload]
  (let [req {:method     "POST"
             :uri        "/"
             :query      ""
             :payload    (util/to-json payload)
             :headers    {"X-Amz-Target" (str "AWSCognitoIdentityProviderService."
                                              command)
                          "Host"         (str "cognito-idp."
                                              region
                                              ".amazonaws.com")
                          "Content-Type" "application/x-amz-json-1.1"
                          "X-Amz-Date"   (common/create-date)}
             :service    "cognito-idp"
             :region     region
             :access-key aws-access-key-id
             :secret-key aws-secret-access-key}
        auth (common/authorize req)

        url
        (str "https://" (get (:headers req) "Host"))

        response (client/retry-n
                  #(util/http-post
                    url
                    (client/request->with-timeouts
                     %
                     {:body    (:payload req)
                      :headers (-> (:headers req)
                                   (dissoc "Host")
                                   (assoc
                                    "X-Amz-Security-Token" aws-session-token
                                    "Authorization" auth))})))
        status (long (:status response))]
    (log/debug "Auth response" response)
    (cond
      (contains? response :error) (do
                                    (log/error "Failed update" response)
                                    {:error (:error response)})
      (> status 299) (do
                       (log/error "Auth failure response"
                                  status
                                  (:body response))
                       {:error {:status status
                                :url url
                                :message (:body response)}})
      :else response)))

(defn admin-initiate-auth
  [{{username :username password :password} :svc
    {client-id     :client-id
     client-secret :client-secret
     user-pool-id  :user-pool-id}           :auth
    :as                                     ctx}]

  (let [{:keys [error] :as response}
        (cognito-request
         ctx
         "AdminInitiateAuth"
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
    :as                          ctx} {:keys [username]}]

  (let [{:keys [error] :as response}
        (cognito-request
         ctx
         "AdminGetUser"
         {:Username   username
          :UserPoolId user-pool-id})]
    (if error
      response
      (get-in response [:body]))))

(defn admin-list-groups-for-user
  [{{user-pool-id :user-pool-id} :auth
    :as                          ctx} {:keys [username]}]

  (let [{:keys [error] :as response}
        (cognito-request
         ctx
         "AdminListGroupsForUser"
         {:Username   username
          :UserPoolId user-pool-id
          :Limit      60})]
    (if error
      response
      (get-in response [:body]))))

