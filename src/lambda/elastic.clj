(ns lambda.elastic
  (:import (java.net.http HttpClient
                          HttpClient$Version
                          HttpRequest
                          HttpRequest$Builder
                          HttpRequest$BodyPublisher HttpRequest$BodyPublishers HttpResponse$BodyHandlers)
           (java.net URI)
           (java.time.format DateTimeFormatter)
           (java.time OffsetDateTime ZoneOffset)
           (javax.script ScriptEngineManager))
  (:require
    [clj-aws-sign.core :as awssign]
    [lambda.util :as util]
    [clojure.pprint :refer [pprint]]
    [clojure.tools.logging :as log]
    [clojure.pprint :refer [pprint]]
    [clojure.string :refer [join]]))

(defn create-date
  []
  (.format
    (. DateTimeFormatter ofPattern "yyyyMMdd'T'HHmmss'Z'")
    (. OffsetDateTime now (. ZoneOffset UTC))))

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
                      :timeout   10000
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
      (log/info "Auth response" (:error response) (:status response))
      (log/info response)
      (cond
        (contains? response :error) (do
                                      (log/error "Failed update" response)
                                      {:error {:error response}})
        (> (:status response) 299) {:error {:status (:status response)}}
        :else (util/to-edn (:body response))))))




