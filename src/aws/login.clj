(ns aws.login
  (:require
   [clojure.tools.logging :as log]
   [lambda.util :as util]))

(defn get-id-token
  "
  Having a username and a password, obtain a token string
  which is passed into the X-Authorization HTTP header.
  The `hosted-zone-name` parameter is a string like
  'lime-dev19.internal.rbigroup.cloud' mostly taken
  from the `PrivateHostedZoneName` env var.

  Return a token value as a string.
  "
  ^String [^String username
           ^String password
           ^String hosted-zone-name]

  (log/infof "Getting AWS login, username: %s, hosted zone name: %s "
             username, hosted-zone-name)

  (let [service
        "glms-login-svc"

        body
        {:username username
         :password password}

        url
        (format "https://%s.%s" service hosted-zone-name)

        query-params
        {"json" "true"
         "user-password-auth" "true"}

        params
        {:query-params query-params
         :body (util/to-json body)}

        response
        (util/http-get url params {:raw true})

        {:keys [status body]}
        response

        ok?
        (<= 200 status 299)]

    (if ok?
      (-> body util/to-edn :id-token)
      (throw (ex-info (format "GLMS login service error, status: %s, url: %s, user: %s"
                              status url username)
                      {:username username
                       :url url
                       :status status
                       :body body})))))
