(ns edd.java-lambda-runtime.core
  (:require
   [aws.lambda :as lambda]
   [aws.ctx :as aws-ctx]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [lambda.emf :as emf]
   [lambda.util :as util]
   [lambda.uuid :as uuid])
  (:import
   [com.amazonaws.services.lambda.runtime Context]))

(defn initialize-system
  []
  (emf/start-metrics-publishing!)
  (atom {}))

(defonce init-cache
  (initialize-system))

(defn java-request-handler
  [init-ctx handler & {:keys [filters post-filter]
                       :or   {filters     []
                              post-filter (fn [ctx] ctx)}}]

  (fn [_this input output ^Context context]
    (binding [util/*cache* init-cache]
      (let [init-ctx
            (if (:aws @init-cache)
              (assoc  init-ctx
                      :aws
                      (:aws @init-cache))
              init-ctx)

            request
            {:body
             (util/to-edn
              (slurp input))}

            invocation-id
            (.getAwsRequestId context)

            init-ctx
            (aws-ctx/init init-ctx)]

        (swap! init-cache
               assoc
               :aws
               (:aws init-ctx))

        (lambda/send-response
         (lambda/handle-request
          (-> init-ctx
              (assoc :filters filters
                     :handler handler
                     :post-filter post-filter)
              (lambda/initalize-context)
              (assoc :from-api (lambda/is-from-api request))
              (assoc :invocation-id (if-not (int? invocation-id)
                                      (uuid/parse invocation-id)
                                      invocation-id)))
          request)

         {:on-success-fn
          (fn [_ctx
               response]
            (with-open [o (io/writer output)]
              (.write o (util/to-json response))))
          :on-error-fn
          (fn [_ctx
               response]
            (with-open [o (io/writer output)]
              (.write o (util/to-json response))))})))))

(defmacro start
  [ctx handler & other]
  (let [this-sym# (gensym "this")
        input-stream-sym# (gensym "input-stream")
        output-stream-sym# (gensym "output-stream")
        lambda-context-sym# (gensym "lambda-context")
        crac-context-sym# (gensym "crac-context")]
    `(do
       (gen-class
        :name "lambda.Handler"
        :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler
                     org.crac.Resource]
        :prefix "-"
        :main false)

       (def ~'-handleRequest
         (fn
           [~this-sym#
            ^java.io.InputStream ~input-stream-sym#
            ^java.io.OutputStream ~output-stream-sym#
            ^com.amazonaws.services.lambda.runtime.Context ~lambda-context-sym#]
           (clojure.tools.logging/info "lambda.Handler -handleRequest invoked.")
           (let [actual-handler-fn# (edd.java-lambda-runtime.core/java-request-handler ~ctx ~handler ~@other)]
             (actual-handler-fn# ~this-sym# ~input-stream-sym# ~output-stream-sym# ~lambda-context-sym#))))

       (def ~'-beforeCheckpoint
         (fn
           [~this-sym# ^org.crac.Context ~crac-context-sym#]
           (clojure.tools.logging/info "lambda.Handler -beforeCheckpoint invoked.")
           (System/gc)))

       (def ~'-afterRestore
         (fn
           [~this-sym# ^org.crac.Context ~crac-context-sym#]
           (clojure.tools.logging/info "lambda.Handler -afterRestore invoked."))))))
