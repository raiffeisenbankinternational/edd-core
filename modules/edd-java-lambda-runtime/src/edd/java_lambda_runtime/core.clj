(ns edd.java-lambda-runtime.core
  (:require
   [aws.lambda :as lambda]
   [aws.ctx :as aws-ctx]
   [lambda.ctx :as lambda-ctx]
   [lambda.metrics :as metrics]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [lambda.logging]
   [lambda.logging.state :as log-state]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [lambda.request :as request]
   [edd.core :as edd]
   [edd.el.cmd :as el-cmd]
   [edd.memory.view-store :as memory-view-store]
   [edd.memory.event-store :as memory-event-store]
   [edd.response.cache :as response-cache]
   [lambda.test.fixture.state :refer [*dal-state* *queues*]])
  (:import
   [com.amazonaws.services.lambda.runtime Context]
   [org.crac Core]))

(defonce init-cache
  (atom {}))

(defonce restore-ctx
  (atom nil))

(defn- ensure-metrics-started!
  "Starts metrics publishing on first invocation. No-op on subsequent calls."
  [ctx]
  (when-not (:metrics-started @init-cache)
    (metrics/start-metrics-publishing! ctx)
    (swap! init-cache assoc :metrics-started true)))

(defn- creds-stale?
  "Returns true when cached AWS credentials expire within the next 5 minutes,
   or when no expiration is stored (conservative: treat as stale)."
  [cached-aws]
  (if-let [exp (:aws-expiration cached-aws)]
    (try
      (let [expiry-ms
            (-> exp java.time.Instant/parse .toEpochMilli)

            buffer-ms
            (* 5 60 1000)]
        (< expiry-ms (+ (System/currentTimeMillis) buffer-ms)))
      (catch Exception _
        (log/warn "Could not parse :aws-expiration, treating credentials as stale:" exp)
        true))
    false))

(defn- warm-up-ctx
  "Builds a minimal in-memory context with a dummy command registered.
   Uses only memory stores — no AWS calls are made."
  [base-ctx]
  (-> base-ctx
      (assoc :meta {:realm :warmup})
      (memory-view-store/register)
      (memory-event-store/register)
      (response-cache/register-default)
      (edd/reg-cmd :warmup-cmd
                   (fn [_ctx cmd]
                     {:event-id :warmup-done
                      :id (:id cmd)})
                   :consumes [:map
                              [:cmd-id [:= :warmup-cmd]]
                              [:id uuid?]])
      (edd/reg-event :warmup-done
                     (fn [agg _event]
                       (assoc agg :warmed true)))))

(defn run-warm-up!
  "Dispatches a synthetic command through the full in-memory pipeline.
   Forces loading of edd.core, edd.el.cmd/event/query, edd.search multimethods,
   edd.dal multimethods, Malli schema compilation, response-cache code paths, etc.
   Must never throw — failures are logged and swallowed."
  [base-ctx]
  (try
    (log/info "SnapStart warm-up: starting pipeline dry-run.")
    (let [ctx
          (warm-up-ctx base-ctx)]
      (binding [*dal-state* (atom {:realm :warmup
                                   :realms {:warmup {}}})
                *queues* {:command-queue (atom [])
                          :seed 0}
                util/*cache* init-cache
                request/*request* (atom {})
                log-state/*invocation-start-ns* (System/nanoTime)]
        (edd/with-stores
          ctx
          #(el-cmd/handle-commands
            %
            {:request-id     (uuid/gen)
             :interaction-id (uuid/gen)
             :commands       [{:cmd-id :warmup-cmd
                               :id     (uuid/gen)}]}))))
    (log/info "SnapStart warm-up: pipeline dry-run complete.")
    (catch Exception e
      (log/warn "SnapStart warm-up failed (non-fatal):" (ex-message e)))))

(defn java-request-handler
  [init-ctx handler & {:keys [filters post-filter]
                       :or   {filters     []
                              post-filter (fn [ctx] ctx)}}]

  (fn [_this input output ^Context context]
    (binding [util/*cache* init-cache
              request/*request* (atom {})
              log-state/*invocation-start-ns* (System/nanoTime)]
      (let [cached-aws
            (:aws @init-cache)

            init-ctx
            (if (and cached-aws (not (creds-stale? cached-aws)))
              (assoc init-ctx
                     :aws cached-aws
                     :aws-ctx-initialized true)
              init-ctx)

            request
            {:body
             (util/to-edn
              (slurp input))}

            invocation-id
            (.getAwsRequestId context)

            init-ctx
            (-> init-ctx
                (assoc :filters filters
                       :handler handler
                       :post-filter post-filter)
                (lambda-ctx/init)
                (aws-ctx/init)
                (lambda/init-filters))]

        (ensure-metrics-started! init-ctx)

        (swap! init-cache
               assoc
               :aws
               (:aws init-ctx))

        (lambda/send-response
         (lambda/handle-request
          (-> init-ctx
              (assoc :from-api (lambda/is-from-api request))
              (assoc :invocation-id (if-not (int? invocation-id)
                                      (uuid/parse invocation-id)
                                      invocation-id)))
          request)

         {:on-success-fn
          (fn [_ctx
               response]
            (log/info "OnSuccessFn writing success")
            (with-open [o (io/writer output)]
              (.write o (util/to-json response))))
          :on-error-fn
          (fn [_ctx
               response]
            (log/info "OnErrorFn writing error")
            (with-open [o (io/writer output)]
              (.write o (util/to-json response)))
            (throw (RuntimeException. (util/to-json response))))})))))

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
        :post-init "post-init"
        :main false)

       (def ~'-post-init
         (fn [~this-sym#]
           (.register (org.crac.Core/getGlobalContext) ~this-sym#)
           (clojure.tools.logging/info "lambda.Handler registered with CRaC global context.")))

       (def ~'-handleRequest
         (fn
           [~this-sym#
            ^java.io.InputStream ~input-stream-sym#
            ^java.io.OutputStream ~output-stream-sym#
            ^com.amazonaws.services.lambda.runtime.Context ~lambda-context-sym#]
           (clojure.tools.logging/info "lambda.Handler -handleRequest invoked.")
           (let [handler-fn#
                 (or (:handler-fn @edd.java-lambda-runtime.core/init-cache)
                     (let [f# (edd.java-lambda-runtime.core/java-request-handler ~ctx ~handler ~@other)]
                       (swap! edd.java-lambda-runtime.core/init-cache assoc :handler-fn f#)
                       f#))]
             (handler-fn# ~this-sym# ~input-stream-sym# ~output-stream-sym# ~lambda-context-sym#))))

       (def ~'-beforeCheckpoint
         (fn
           [~this-sym# ^org.crac.Context ~crac-context-sym#]
           (clojure.tools.logging/info "lambda.Handler -beforeCheckpoint invoked.")
           (reset! edd.java-lambda-runtime.core/restore-ctx ~ctx)
           (edd.java-lambda-runtime.core/run-warm-up! @edd.java-lambda-runtime.core/restore-ctx)
           (swap! edd.java-lambda-runtime.core/init-cache dissoc :aws)
           (clojure.tools.logging/info "lambda.Handler -beforeCheckpoint: stale credentials cleared, forcing GC.")
           (System/gc)))

       (def ~'-afterRestore
         (fn
           [~this-sym# ^org.crac.Context ~crac-context-sym#]
           (clojure.tools.logging/info "lambda.Handler -afterRestore invoked. Refreshing credentials.")
           (swap! edd.java-lambda-runtime.core/init-cache dissoc :aws :aws-ctx-initialized :metrics-started)
           (try
             (let [fresh-ctx#
                   (-> @edd.java-lambda-runtime.core/restore-ctx
                       (dissoc :aws-ctx-initialized)
                       (aws.ctx/init))]
               (swap! edd.java-lambda-runtime.core/init-cache assoc :aws (:aws fresh-ctx#))
               (clojure.tools.logging/info "lambda.Handler -afterRestore: credentials refreshed."))
             (catch Exception e#
               (clojure.tools.logging/warn "lambda.Handler -afterRestore: credential refresh failed (will retry on first invocation):"
                                           (ex-message e#)))))))))
