(ns runtime.aws
  (:require [lambda.core :as core]
            [aws.lambda :as lambda]
            [aws.aws :as aws]
            [clojure.tools.logging :as log]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [lambda.request :as request]
            [clojure.java.io :as io])
  (:gen-class
   :constructors {[] []}
   :init init
   :state state
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:import (com.amazonaws.services.lambda.runtime Context)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn send-error
  [{:keys [from-api]} e]
  (log/error e "Error processing request")
  (if from-api
    e
    (throw (ex-info "Error handling"
                    e))))

(defn send-success
  [{:keys [from-api] :as ctx} filtered]
  (log/info "Response from-api?" from-api)
  (util/d-time "Enqueueing success"
               (aws/enqueue-response ctx filtered))
  (assoc ctx :resp filtered))

(defn send-response
  [{:keys [resp] :as ctx}]
  (let [exception (if (vector? resp)
                    (-> (filter #(contains? % :exception) resp)
                        (first))
                    (:exception resp))
        filtered (:resp (lambda/apply-post-filter ctx))]
    (if exception
      (send-error ctx filtered)
      (send-success ctx filtered))))

(defn apply-post-filter
  [{:keys [post-filter] :as ctx}]
  (:resp (post-filter ctx)))

(defn lambda-request-handler
  [ctx request]
  (aws.lambda/with-cache
    #(let [from-api (aws.lambda/is-from-api request)
           invocation-id (get-in
                          request
                          [:headers :lambda-runtime-aws-request-id])]
       (util/d-time
        (str "handling-request-" invocation-id "-form-api-" from-api)
        (binding [request/*request* (atom {:scoped true})]
          (send-response
           (lambda/handle-request
            (-> ctx
                (assoc :from-api from-api)
                (assoc :invocation-id (if-not (int? invocation-id)
                                        (uuid/parse invocation-id)
                                        invocation-id)))
            request)))))))

(defn -init
  "Constructor"
  []
  (let [register-symbol (str (util/get-property "edd.main-class")
                             "/-main")
        init-ctx (atom {})]
    (if-let [main-function (ns-resolve *ns*
                                       (symbol register-symbol))]
      (with-redefs [core/start
                    (fn [ctx handler & {:keys [filters post-filter]
                                        :or   {filters     []
                                               post-filter (fn [ctx] ctx)}}]
                      (log/info "Initializing context")
                      (let [ctx (-> ctx
                                    (assoc :filters filters
                                           :handler handler
                                           :post-filter post-filter)
                                    (lambda/initalize-context))]
                        (reset! init-ctx ctx)))]
        (apply main-function {}))
      (let [^String message (format "Unable ro find register method %s"
                                    register-symbol)]
        (throw (RuntimeException. message))))
    [[] init-ctx]))

(defn -handleRequest [this input output ^Context context]
  (let [req (util/to-edn
             (slurp input))
        req {:body req}
        req (assoc-in req
                      [:headers :lambda-runtime-aws-request-id]
                      (.getAwsRequestId context))
        _ (log/info (.state this))
        _ (log/info :request req)
        ctx @(.state this)]
    (let [{:keys [resp]} (lambda-request-handler ctx req)
          resp (util/to-json resp)]
      (log/info :response resp)
      (with-open [o (io/writer output)]
        (.write o (str resp))))))

