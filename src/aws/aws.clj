(ns aws.aws
  (:require
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [lambda.request :as request]
   [sdk.aws.common :as common]
   [sdk.aws.cognito-idp :as cognito-idp]
   [sdk.aws.sqs :as sqs]
   [clojure.string :as string]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

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

(defn enqueue-response
  [ctx _]
  (let [resp (get @request/*request* :cache-keys)]
    (when resp
      (doall
       (map
        #(let [{:keys [error]} (sqs/sqs-publish
                                (assoc ctx :queue (str
                                                   (:environment-name-lower ctx)
                                                   "-glms-router-svc-response")
                                       :message (util/to-json
                                                 {:Records [%]})))]
           (when error
             (throw (ex-info "Distribution failed" error))))
        (flatten resp))))))

(defn send-loop-runtime-success
  [{:keys [api
           invocation-id]
    :as _ctx}
   response]
  (util/http-post
   (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/response")
   {:body (util/to-json response)}))

(defn send-success
  [{:keys [from-api] :as ctx}
   response
   & [{:keys [on-success-fn]
       :or {on-success-fn send-loop-runtime-success}}]]

  (util/d-time (str "Distributing success (from-api? " from-api ")")
               (enqueue-response ctx response))
  (on-success-fn ctx response))

(defn get-items-to-delete
  [responses records]
  (loop [responses responses
         records records
         to-delete []]

    (let [{:keys [exception]
           :as response}
          (first responses)

          record
          (first records)]
      (cond
        (and response
             record
             (not exception))
        (recur (rest responses)
               (rest records)
               (conj to-delete record))
        :else
        to-delete))))

(defn send-loop-runtime-error
  [{:keys [api
           from-api
           invocation-id]
    :as _ctx}
   response]
  (let [target
        (if from-api
          "response"
          "error")]
    (util/http-post
     (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/" target)
     {:body (util/to-json response)})))

(defn send-error
  [{:keys [from-api
           req]
    :as ctx}
   response

   & [{:keys [on-error-fn]
       :or {on-error-fn send-loop-runtime-error}}]]

  (when-not from-api
    (let [responses
          (if (map? response)
            (do
              (log/info "Got map as queu response :S")
              [response])
            response)

          records
          (:Records req)

          to-delete
          (get-items-to-delete responses
                               records)]
      (when (> (count to-delete) 0)
        (let [queue (-> to-delete
                        first
                        :eventSourceARN
                        (string/split #":")
                        last)]
          (sqs/delete-message-batch (assoc ctx
                                           :queue queue
                                           :messages to-delete))))))
  (util/d-time "Distribute error"
               (enqueue-response ctx response))
  (log/error response)
  (on-error-fn ctx response))

(defn get-or-set
  [cache key get-fn]
  (let [current-time (long (util/get-current-time-ms))
        meta (get-in cache [:meta key])]
    (if (or (not (get cache key))
            (> (- current-time (long (get meta :time 0))) 1800000))
      (-> cache
          (assoc-in [:meta key] {:time current-time})
          (assoc key (common/retry
                      get-fn
                      3)))
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
