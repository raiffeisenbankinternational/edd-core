(ns aws.lambda
  (:require [lambda.util :as util]
            [aws.aws :as aws]
            [lambda.emf :as emf]
            [lambda.request :as request]
            [lambda.uuid :as uuid]
            [aws.ctx :as aws-ctx]
            [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn apply-filters
  [{:keys [filters req] :as ctx}]
  (reduce
   (fn [c {:keys [cond fn]}]
     (if (cond c)
       (fn c)
       c))
   (assoc ctx
          :body
          req)
   filters))

(defn apply-post-filter
  [{:keys [post-filter] :as ctx}]
  (post-filter ctx))

(defn send-response
  [{:keys [resp] :as ctx} & params]
  (let [exception (if (vector? resp)
                    (-> (filter #(contains? % :exception) resp)
                        (first))
                    (:exception resp))
        filtered (:resp (apply-post-filter ctx))]
    (if exception
      (aws/send-error ctx filtered params)
      (aws/send-success ctx filtered params))))

(defn invoke-handler
  [{:keys [body handler] :as ctx}]
  (util/d-time
   "time-invoke-handler"
   (cond
     (:error body) (assoc ctx :resp body)
     (:health-check ctx) (assoc ctx :resp {:healthy  true
                                           :build-id (util/get-env "BuildId" "b0")})

     :else (assoc ctx
                  :resp
                  (handler ctx
                           body)))))

(defn handle-error
  [ctx e]
  (log/error e "Error processing request")
  (let [data (ex-data e)]
    (assoc ctx
           :resp {:exception {:message (or data
                                           (ex-message e)
                                           "Unable to parse exception")}})))

(defn with-cache
  [fn]
  (binding [util/*cache* (atom {})]
    (fn)))

(defn get-loop
  "Extracting lambda looping as infinite loop to be able to mock it"
  []
  (range))

(defn init-filters
  [{:keys [filters] :as ctx}]
  (reduce
   (fn [c {:keys [init] :as f}]
     (if init
       (init c)
       c))
   ctx
   filters))

(defn is-from-api
  [request]
  (contains? (:body request) :path))

(defn handle-request
  [ctx req]
  (let [{:keys [error body]} req]
    (if error
      (assoc ctx :resp {:exception error})
      (try
        (-> ctx
            (assoc :req body
                   :lambda-api-requiest req
                   :query-string-parameters (:queryStringParameters body))
            (apply-filters)
            (invoke-handler)
            (doall))
        (catch Exception e
          (handle-error (assoc ctx
                               :req body) e))
        (catch AssertionError e
          (handle-error (assoc ctx
                               :req body) e))))))

(defn initalize-context
  [ctx]
  (-> ctx
      (merge (util/load-config "secret.json"))
      (merge (util/to-edn
              (util/get-env "CustomConfig" "{}")))
      (aws-ctx/init)
      (assoc :service-name (keyword (util/get-env
                                     "ServiceName"
                                     "local-test"))
             :hosted-zone-name (util/get-env
                                "PublicHostedZoneName"
                                "example.com")
             :environment-name-lower (util/get-env
                                      "EnvironmentNameLower"
                                      "local"))
      (init-filters)))

(defn lambda-custom-runtime
  [init-ctx handler & {:keys [filters post-filter]
                       :or   {filters     []
                              post-filter (fn [ctx] ctx)}}]
  ;; a way to disable metrics locally to keep REPL clear
  (when-not (util/get-env "AWS_LAMBDA_DISABLE_METRICS")
    (emf/start-metrics-publishing!))
  (with-cache
    #(let [api (util/get-env "AWS_LAMBDA_RUNTIME_API")
           ctx (-> init-ctx
                   (assoc :filters filters
                          :handler handler
                          :post-filter post-filter)
                   (initalize-context))]
       (doseq [i (get-loop)]
         (let [request (aws/get-next-request api)
               invocation-id (get-in
                              request
                              [:headers :lambda-runtime-aws-request-id])]

           (util/d-time
            (str "Handling next request: " i ", invocation-id: " invocation-id)
            (binding [request/*request* (atom {:scoped true})]
              (send-response
               (handle-request
                (-> ctx
                    (assoc :from-api (is-from-api request))
                    (assoc :api api
                           :invocation-id (if-not (int? invocation-id)
                                            (uuid/parse invocation-id)
                                            invocation-id)))
                request)))))))))
