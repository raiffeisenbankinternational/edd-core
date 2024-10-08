(ns lambda.elastic
  (:require
   [lambda.http-client :as client]
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [clojure.string :refer [join]]
   [sdk.aws.common :as common]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn get-env
  [name & [default]]
  (get (System/getenv) name default))

(defn query
  [{:keys [method path body elastic-search aws]} & {:keys [ignored-status]}]
  (let [req (cond-> {:method     method
                     :uri        path
                     :query      ""
                     :headers    {"Host"         (:url elastic-search)
                                  "Content-Type" "application/json"
                                  "X-Amz-Date"   (common/create-date)}
                     :service    "es"
                     :region     (:region aws)
                     :access-key (:aws-access-key-id aws)
                     :secret-key (:aws-secret-access-key aws)}
              body (assoc :payload body))
        auth (common/authorize req)
        headers (-> (:headers req)
                    (dissoc "Host")
                    (assoc "Authorization" auth))
        headers (cond-> headers
                  (:aws-session-token aws) (assoc "X-Amz-Security-Token" (:aws-session-token aws)))
        request (cond-> {:headers  headers
                         :keepalive 300000}
                  body (assoc :body body))]

    (let [url (str (or (:scheme elastic-search) "https")
                   "://"
                   (get (:headers req) "Host")
                   (:uri req))
          response (client/retry-n
                    #(let [request (client/request->with-timeouts
                                    %
                                    request
                                    :idle-timeout 20000)]
                       (cond
                         (= method "GET") (util/http-get
                                           url
                                           request
                                           :raw true)
                         (= method "POST") (util/http-post
                                            url
                                            request
                                            :raw true)
                         (= method "PUT") (util/http-put
                                           url
                                           request
                                           :raw true)
                         (= method "DELETE") (util/http-delete
                                              url
                                              request
                                              :raw true))))
          status (long (:status response))
          ignored-status (long (or ignored-status 0))]
      (cond
        (contains? response :error) (do
                                      (log/warn "Failed update, client should handle error" response)
                                      {:error {:error response}})
        (= status ignored-status) nil
        (> status 299) (do
                         {:error {:message (:body response)
                                  :status  (:status response)}})
        :else (util/to-edn (:body response))))))
