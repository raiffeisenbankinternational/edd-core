(ns aws.aws
  (:require
   [lambda.util :as util]
   [clojure.tools.logging :as log]
   [lambda.request :as request]
   [sdk.aws.common :as common]
   [clojure.set :as clojure-set]
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

(def response
  {:statusCode 200
   :headers    {"Content-Type" "application/json"}})

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

(defn send-success
  [{:keys [api
           invocation-id
           from-api] :as ctx} body]

  (util/d-time (str "Distributing success (from-api? " from-api ")")
               (enqueue-response ctx body))
  (util/to-json
   (util/http-post
    (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/response")
    {:body (util/to-json body)})))

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

(defn send-error
  [{:keys [api
           invocation-id
           from-api
           req]
    :as ctx}

   responses]

  (let [target (if from-api
                 "response"
                 "error")]
    (when-not from-api
      (let [responses
            (if (map? responses)
              (do
                (log/info "Got map as queu response :S")
                [responses])
              responses)

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

    (let [resp (util/to-json responses)]
      (util/d-time "Distribute error"
                   (enqueue-response ctx responses))
      (log/error resp)
      (util/to-json
       (util/http-post
        (str "http://" api "/2018-06-01/runtime/invocation/" invocation-id "/" target)
        {:body resp})))))

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
