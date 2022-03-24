(ns aws.aws
  (:require
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [lambda.request :as request]
   [sdk.aws.common :as common]
   [clojure.set :as clojure-set]
   [sdk.aws.cognito-idp :as cognito-idp]
   [sdk.aws.sqs :as sqs]))

(defn get-next-invocation
  [runtime-api]
  (util/http-get
   (str "http://" runtime-api "/2018-06-01/runtime/invocation/next")
   {:timeout 90000000}))

(defn get-next-request [runtime-api]
  (let [req (get-next-invocation runtime-api)]
    (if (-> req :body :isBase64Encoded)
      (update-in req [:body :body] util/base64decode)
      req)))

(def response
  {:statusCode 200
   :headers    {"Content-Type" "application/json"}})

(defn enqueue-response
  [ctx _]
  (let [resp (get @request/*request* :cache-keys)]
    (when resp
      (log/info "Distributing response")
      (doall
       (map
        #(let [{:keys [error]} (sqs/sqs-publish
                                (assoc ctx :queue "glms-router-svc-response"
                                       :message (util/to-json
                                                 {:Records [%]})))]
           (when error
             (throw (ex-info "Distribution failed" error))))
        (flatten resp))))))

(defn send-success
  [{:keys [api
           invocation-id
           from-api] :as ctx} body]

  (log/info "Response from-api?" from-api)
  (util/d-time "Enqueueing success"
               (enqueue-response ctx body))
  (util/to-json
   (util/http-post
    (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/response")
    {:body (util/to-json body)})))

(defn produce-compatible-error-response
  "Because we rely on error on client we will replace :exception to :error"
  [resp]
  (if (map? resp)
    (clojure-set/rename-keys resp {:exception :error})
    (map
     #(clojure-set/rename-keys % {:exception :error})
     resp)))

(defn send-error
  [{:keys [api
           invocation-id
           from-api
           req] :as ctx} body]
  (let [target (if from-api
                 "response"
                 "error")]
    (when-not from-api
      (let [items (interleave body
                              (:Records req))
            items-to-delete (->> (partition 2 items)
                                 (filter (fn [[a _]]
                                           (not (:exception a))))
                                 (map
                                  (fn [[_ b]]
                                    b)))]
        (when (and (> (count items-to-delete) 0)
                   (= (count body)
                      (count (:Records req))))
          (sqs/delete-message-batch ctx items-to-delete))))

    (util/d-time "Enqueueing error"
                 (enqueue-response ctx body))

    (let [body (produce-compatible-error-response body)
          resp (util/to-json body)]
      (log/error resp)
      (util/to-json
       (util/http-post
        (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/" target)
        {:body resp})))))

(defn get-or-set
  [cache key get-fn]
  (let [current-time (util/get-current-time-ms)
        meta (get-in cache [:meta key])]
    (if (or (not (get cache key))
            (> (- current-time (get meta :time 0)) 1800000))
      (do
        (-> cache
            (assoc-in [:meta key] {:time current-time})
            (assoc key (common/retry
                        get-fn
                        3))))
      cache)))

(defn admin-auth
  [ctx]
  (let [{:keys [error] :as response} (cognito-idp/admin-initiate-auth ctx)]
    (if error
      response
      (get-in response [:AuthenticationResult :IdToken]))))

(defn get-token-from-cache
  [ctx]
  (let [{:keys [id-token]} (swap! util/*cache*
                                  (fn [cache]
                                    (get-or-set cache
                                                :id-token
                                                (fn []
                                                  (admin-auth ctx)))))]
    id-token))

(defn get-token
  [ctx]
  (if (:id-token ctx)
    (:id-token ctx)
    (get-token-from-cache ctx)))
