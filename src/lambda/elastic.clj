(ns lambda.elastic
  (:import (java.time.format DateTimeFormatter)
           (java.time OffsetDateTime ZoneOffset))
  (:require
   [clj-aws-sign.core :as awssign]
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [clojure.string :refer [join]]
   [aws :as aws]))

(defn create-date
  []
  (aws/create-date))

(defn get-env
  [name & [default]]
  (get (System/getenv) name default))

(defn query
  [{:keys [method path body elastic-search aws]}]
  (let [req (cond-> {:method     method
                     :uri        path
                     :query      ""
                     :headers    {"Host"         (:url elastic-search)
                                  "Content-Type" "application/json"
                                  "X-Amz-Date"   (create-date)}
                     :service    "es"
                     :region     (:region aws)
                     :access-key (:aws-access-key-id aws)
                     :secret-key (:aws-secret-access-key aws)}
              body (assoc :payload body))
        auth (awssign/authorize req)
        body (cond-> {:headers   (-> (:headers req)
                                     (dissoc "Host")
                                     (assoc
                                      "X-Amz-Security-Token" (:aws-session-token aws)
                                      "Authorization" auth))
                      :timeout   20000
                      :keepalive 300000}
               body (assoc :body body))]

    (let [url (str "https://"
                   (get (:headers req) "Host")
                   (:uri req))
          response (cond
                     (= method "POST") (util/http-post
                                        url
                                        body
                                        :raw true)
                     (= method "PUT") (util/http-put
                                       url
                                       body
                                       :raw true)
                     (= method "DELETE") (util/http-delete
                                          url
                                          body
                                          :raw true))]
      (cond
        (contains? response :error) (do
                                      (log/error "Failed update" response)
                                      {:error {:error response}})
        (> (:status response) 299) (do
                                     (log/error "Elastic query" body)
                                     (log/error "Elastic response"
                                                (:status response)
                                                (:body response))
                                     {:error {:status (:status response)}})
        :else (util/to-edn (:body response))))))




