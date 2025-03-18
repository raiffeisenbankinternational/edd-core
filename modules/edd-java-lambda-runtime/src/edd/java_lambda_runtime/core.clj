(ns edd.java-lambda-runtime.core
  (:require
   [aws.aws :as aws]
   [aws.lambda :as core]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [edd.core :as edd]
   [lambda.emf :as emf]
   [lambda.filters :refer [from-api to-api]]
   [lambda.request :as request]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [sdk.aws.s3 :as s3]
   [edd.java-lambda-runtime.crac])
  (:import
   [com.amazonaws.services.lambda.runtime Context RequestStreamHandler]
   [java.io InputStream OutputStream]))

(defn custom-handler
  [ctx body]
  (edd/handler ctx body))

(defn handle-error
  [{:keys [from-api]} e]
  (log/error e "Error processing request")
  (if from-api
    e
    (throw (ex-info "Error handling"
                    e))))

(defn send-success
  [{:keys [api
           invocation-id
           from-api
           resp] :as ctx}]

  (log/info "Response from-api?" from-api)
  (util/d-time "Enqueueing success"
               (aws/enqueue-response ctx resp))
  ctx)

(defn apply-post-filter
  [{:keys [post-filter] :as ctx}]
  (:resp (post-filter ctx)))

(defn handle-request
  [ctx req]
  (let [{:keys [error body]} req]
    (log/debug "Request body" (util/to-json body))
    (if error
      (do
        (log/error "Request failed" (util/to-json req))
        (handle-error
         ctx
         (assoc ctx
                :resp {:error error})))
      (try
        (-> ctx
            (assoc :req body
                   :query-string-parameters (:queryStringParameters body))
            (core/apply-filters)
            (core/invoke-handler)
            (send-success)
            (apply-post-filter)
            (doall))
        (catch Throwable e
          (handle-error (assoc ctx
                               :req body) e))))))

(defn lambda-request-handler
  [init-ctx handler request & {:keys [filters post-filter]
                               :or   {filters     []
                                      post-filter (fn [ctx] ctx)}}]
  (core/with-cache
    #(let [api (util/get-env "AWS_LAMBDA_RUNTIME_API")]
       (with-redefs [util/load-config (if-not (= api "mock") util/load-config (fn [_] {}))]
         (let [ctx (-> init-ctx
                       (merge (util/load-config "secret.json"))
                       (assoc :filters filters
                              :handler handler
                              :post-filter post-filter)
                       (merge (util/to-edn
                               (util/get-env "CustomConfig" "{}")))
                       (assoc :service-name (keyword (util/get-env
                                                      "ServiceName"
                                                      "local-test"))
                              :aws {:region                (util/get-env "Region" "local")
                                    :account-id            (util/get-env "AccountId" "local")
                                    :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID" "")
                                    :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY" "")
                                    :aws-session-token     (util/get-env "AWS_SESSION_TOKEN" "")}
                              :hosted-zone-name (util/get-env
                                                 "PublicHostedZoneName"
                                                 "example.com")
                              :environment-name-lower (util/get-env
                                                       "EnvironmentNameLower"
                                                       "local"))
                       (core/init-filters))
               from-api (core/is-from-api request)
               invocation-id (get-in
                              request
                              [:headers :lambda-runtime-aws-request-id])]
           (util/d-time
            (str "handling-request-" invocation-id "-form-api-" from-api)
            (binding [request/*request* (atom {:scoped true})]
              (handle-request
               (-> ctx
                   (assoc :from-api from-api)
                   (assoc :api api
                          :invocation-id (if-not (int? invocation-id)
                                           (uuid/parse invocation-id)
                                           invocation-id)))
               request))))))))

(defn fetch-from-s3
  [ctx {:keys [s3]
        :as msg}]
  (if s3
    (let [maybe-result (s3/get-object ctx msg)]
      (if (and (map? maybe-result)
               (:error maybe-result))
        (do
          (log/error "Error fetching from S3" (:error maybe-result))
          (throw (ex-info "Propagate S3 fetch error" (:error maybe-result))))
        (let [result (slurp maybe-result)]
          (util/to-edn result))))
    msg))

(def custom-from-queue
  {:cond (fn [{:keys [body]}]
           (if (and
                (contains? body :Records)
                (= (:eventSource (first (:Records body))) "aws:sqs"))
             true
             false))
   :fn   (fn [{:keys [body] :as ctx}]
           (assoc ctx
                  :body  (->> body
                              (:Records)
                              (first)
                              (:body)
                              util/to-edn
                              (fetch-from-s3 ctx))))})

(def metrics-initialized? (volatile! false))

(defmacro register-java-runtime!
  [ctx]
  `(do
     (gen-class
      :name "lambda.Handler"
      :implements [RequestStreamHandler])
     (def ~'-handleRequest
       (fn [_# ^InputStream input# ^OutputStream output# ^Context context#]
         (when-not @metrics-initialized?
           (vreset! metrics-initialized? true)
           (emf/start-metrics-publishing!))
         (let [req# (util/to-edn (slurp input#))
               req# {:body req#}
               req# (assoc-in req# [:headers :lambda-runtime-aws-request-id] (.getAwsRequestId context#))
               ctx# (if (fn? ~ctx) (~ctx) ~ctx)
               resp# (lambda-request-handler
                      ctx#
                      custom-handler
                      req#
                      :filters (apply conj [from-api custom-from-queue] (:filters ctx#))
                      :post-filter to-api)]
           (with-open [o# (io/writer output#)]
             (util/to-json-out o# resp#)))))))