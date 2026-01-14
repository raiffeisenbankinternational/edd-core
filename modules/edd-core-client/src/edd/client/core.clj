(ns edd.client.core
  "
  HTTP client to reach other services.
  "
  (:import
   java.net.URL)
  (:require
   [aws.aws :as aws]
   [clojure.tools.logging :as log]
   [lambda.util :as util]))

(set! *warn-on-reflection* true)

(def TIMEOUT 5000)

(defn get-service-url
  "
  Build a full internal URL to a service.
  "
  ^String [ctx service endpoint]

  (let [protocol
        (or (get ctx :dev/deps-protocol)
            (util/get-env "DEPS_PROTOCOL" "https"))

        port
        (if (= "https" protocol) 443 8080)

        zone-name
        (or (get ctx :dev/zone-name)
            (util/get-env "PrivateHostedZoneName"))

        host
        (format "%s.%s"
                (name service)
                zone-name)]

    (str (new URL protocol host port endpoint))))

(defn throw-http-error
  "
  Throw an exception handling info about an unsuccessful
  HTTP invocation.
  "
  [url status method service body]
  (let [message (format "HTTP client error, status: %s, service: %s, URL: %s, method: %s"
                        status service url method)]
    (throw (ex-info message {:url url
                             :status status
                             :method method
                             :body body}))))

(defn call-service
  "
  A common invocation function that handles
  various HTTP methods and accepts arbitrary payload.
  Returns the `:result` field of the parsed body
  (can be overriden with result-fn) or throws an
  exception when the status code was not 2xx.
  "
  ([ctx service method endpoint payload]
   (call-service ctx service method endpoint payload nil))

  ([ctx service method endpoint payload
    {:keys [headers
            timeout
            result-fn]
     :or {timeout TIMEOUT
          result-fn :result}}]

   (let [url
         (get-service-url ctx service endpoint)

         method-fn
         (case method
           (:get "GET") util/http-get
           (:post "POST") util/http-post)

         {ctx-meta :meta
          :keys [request-id
                 interaction-id]}
         ctx

         token
         (or (get ctx :dev/token)
             (aws/get-token ctx))

         payload-full
         (assoc payload
                :meta ctx-meta
                :request-id request-id
                :interaction-id interaction-id)

         headers-full
         (assoc headers
                "Content-Type" "application/json"
                "X-Authorization" token
                "Accept-Encoding" "gzip")

         request
         {:body (util/to-json payload-full)
          :headers headers-full
          :timeout timeout}

         _
         (log/infof "Remote service call, service: %s, method: %s, url: %s"
                    service method url)

         response
         (method-fn url request)

         {:keys [status body]}
         response

         {:keys [error
                 exception]}
         body

         status-ok?
         (<= 200 status 299)]

     (cond

       (or (not status-ok?) error exception)
       (throw-http-error url status method service body)

       :else
       (result-fn body)))))

(defn query-service
  "
  A common function to query a service.
  "
  ([ctx service query-id params]
   (query-service ctx service query-id params nil))

  ([ctx service query-id params opt]
   (let [payload
         {:query (assoc params
                        :query-id query-id)}]
     (call-service ctx
                   service
                   :post
                   "/query"
                   payload
                   opt))))

;;
;; Get by ID(s) shortcuts
;;

(defn get-by-id
  "
  The `get-by-id` query common shortcut.
  "
  ([ctx service id]
   (get-by-id ctx service id nil))

  ([ctx service id opt]
   (query-service ctx service :get-by-id {:id id} opt)))

(defn get-by-ids
  "
  The `get-by-ids` query common shortcut.
  "
  ([ctx service ids]
   (get-by-ids ctx service ids nil))

  ([ctx service ids opt]
   (query-service ctx service :get-by-ids {:ids ids} opt)))

(defn get-by-ids-pmap
  "
  A surrogate implementation of `get-by-ids` which acts
  through `get-by-id` and `pmap`. Use with services
  that don't support `get-by-ids` out from the box.
  "
  ([ctx service ids]
   (get-by-ids-pmap ctx service ids nil))

  ([ctx service ids opt]
   (pmap get-by-id
         (repeat ctx)
         (repeat service)
         ids
         (repeat opt))))
