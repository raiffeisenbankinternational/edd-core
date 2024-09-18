(ns lambda.elastic
  (:import
   clojure.lang.Keyword)
  (:require
   [lambda.http-client :as client]
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [clojure.string :as str]
   [sdk.aws.common :as common]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defmacro error! [template & args]
  `(throw (new RuntimeException (format ~template ~@args))))

(defn ->snake-case ^String [x]
  (-> x name (str/replace #"-" "_")))

(defn get-index-name ^Keyword [realm service-name]
  (keyword (format "%s_%s"
                   (->snake-case realm)
                   (->snake-case service-name))))

(defn query
  [{:keys [method path body query elastic-search aws]} & {:keys [ignored-status]}]
  (let [req (cond-> {:method     method
                     :uri        path
                     :headers    {"Host"         (:url elastic-search)
                                  "Content-Type" "application/json"
                                  "X-Amz-Date"   (common/create-date)}
                     :service    "es"
                     :region     (:region aws)
                     :access-key (:aws-access-key-id aws)
                     :secret-key (:aws-secret-access-key aws)}

              body
              (assoc :payload body)

              query
              (assoc :query query))

        auth
        (common/authorize req)

        headers
        (-> (:headers req)
            (dissoc "Host")
            (assoc "Authorization" auth))

        headers
        (cond-> headers
          (:aws-session-token aws)
          (assoc "X-Amz-Security-Token" (:aws-session-token aws)))

        request
        (cond-> {:headers  headers
                 :keepalive 300000}
          body (assoc :body body)
          query (assoc :query-params query))]

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

(defn index-settings
  "
  Get a map of settings for the specified realm and service.
  "
  [ctx realm service]
  (let [{:keys [aws
                elastic-search]}
        ctx

        index
        (get-index-name realm service)

        params
        {:method         "GET"
         :path           (format "/%s/_settings" (name index))
         :elastic-search elastic-search
         :aws            aws}

        {:keys [error] :as resp}
        (query params)]

    (if error
      (error! "Error getting settings: %s" error)
      (get resp index))))

(defn read-only? [settings]
  (-> settings
      (get-in [:settings :index :blocks :read_only])
      (= "true")))
