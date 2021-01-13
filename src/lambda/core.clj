(ns lambda.core
  (:gen-class)
  (:require [lambda.util :as util]
            [aws :as aws]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]))

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
  [{:keys [resp] :as ctx}]
  (let [error (:error resp)
        filtered (:resp (apply-post-filter ctx))]
    (if error
      (aws/send-error ctx filtered)
      (aws/send-success ctx filtered))))

(defn invoke-handler
  [{:keys [body handler] :as ctx}]
  (cond
    (:error body) (assoc ctx :resp body)
    (:health-check ctx) (assoc ctx :resp {:healthy  true
                                          :build-id (util/get-env "BuildId" "b0")})
    :else (assoc ctx
            :resp
            (handler ctx
                     body))))


(defn handle-error
  [ctx e]
  (log/error e "Error processing request")
  (send-response
    (assoc ctx
      :resp {:error (try (.getMessage e)
                         (catch IllegalArgumentException e
                           "Unknown"))})))

(defn handle-request
  [ctx req]
  (let [{:keys [error body]} req]
    (log/debug "Request body" (util/to-json body))
    (if error
      (do
        (log/error "Request failed" (util/to-json req))
        (send-response
          (assoc ctx
            :resp {:error error})))
      (try
        (-> ctx
            (assoc :req body
                   :query-string-parameters (:queryStringParameters body))
            (apply-filters)
            (invoke-handler)
            (send-response)
            (doall))
        (catch Exception e
          (handle-error ctx e))
        (catch AssertionError e
          (handle-error ctx e))))))

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

(defn with-cache
  [fn]
  (binding [util/*cache* (atom {})]
    (fn)))

(defn is-from-api
  [request]
  (contains? (:body request) :path))

(defn start
  [init-ctx handler & {:keys [filters post-filter]
                       :or   {filters     []
                              post-filter (fn [ctx] ctx)}}]

  (with-cache
    #(let [api (util/get-env "AWS_LAMBDA_RUNTIME_API")
           ctx (-> init-ctx
                   (merge (util/load-config "secret.json"))
                   (assoc :filters filters
                          :handler handler
                          :post-filter post-filter)
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
                   (init-filters))]
       (doseq [i (get-loop)]
         (let [request (aws/get-next-request api)]
           (log/debug "Loop" i)
           (handle-request
             (-> ctx
                 (assoc :from-api (is-from-api request))
                 (assoc :api api

                        :invocation-id (get-in
                                         request
                                         [:headers :lambda-runtime-aws-request-id])))
             request))))))


