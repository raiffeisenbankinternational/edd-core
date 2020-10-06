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
  [method path body]
  (let [req {:method     method
             :uri        path
             :query      ""
             :payload    body
             :headers    {"Host"         (util/get-env "IndexDomainEndpoint")
                          "Content-Type" "application/json"
                          "X-Amz-Date"   (create-date)}
             :service    "es"
             :region     "eu-central-1"
             :access-key (util/get-env "AWS_ACCESS_KEY_ID")
             :secret-key (util/get-env "AWS_SECRET_ACCESS_KEY")}
        auth (awssign/authorize req)
        body {:body      (:payload req)
              :headers   (-> (:headers req)
                             (dissoc "Host")
                             (assoc
                               "X-Amz-Security-Token" (util/get-env "AWS_SESSION_TOKEN")
                               "Authorization" auth))
              :timeout   10000
              :keepalive 300000}]

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
                                        :raw true))]
      (log/info "Auth response" (:error response) (:status response))
      (log/info response)
      (cond
        (contains? response :error) (do
                                      (log/error "Failed update" response)
                                      {:error {:error response}})
        (> (:status response) 299) {:error {:status (:status response)}}
        :else (util/to-edn (:body response))))))




