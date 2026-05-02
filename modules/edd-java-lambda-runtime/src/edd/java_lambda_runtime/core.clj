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

(defonce init-cache (atom {}))
(defonce restore-ctx (atom nil))

(defn ms-since [start-ns]
  (/ (- (System/nanoTime) (long start-ns)) 1e6))

(def ^:private creds-buffer-ms (* 5 60 1000))

(defn- creds-stale?
  [aws]
  (if-let [exp (:aws-expiration aws)]
    (try
      (< (-> exp java.time.Instant/parse .toEpochMilli)
         (+ (System/currentTimeMillis) creds-buffer-ms))
      (catch Exception _
        (log/warn "Could not parse :aws-expiration, treating credentials as stale:" exp)
        true))
    false))

(defn- apply-cached-aws [ctx]
  (let [cached
        @init-cache

        aws
        (:aws cached)]
    (if (and aws (not (creds-stale? aws)))
      (assoc ctx :aws aws :aws-ctx-initialized true)
      ctx)))

(defn- cache-aws! [ctx]
  (swap! init-cache assoc
         :aws (:aws ctx)
         :aws-ctx-initialized true))

(defn- ensure-metrics! [ctx]
  (when-not (:metrics-started @init-cache)
    (metrics/start-metrics-publishing! ctx)
    (swap! init-cache assoc :metrics-started true)))

(defn- warm-up-ctx [ctx]
  (-> ctx
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

(defn run-warm-up! [base-ctx]
  (try
    (let [preloaded
          (-> base-ctx
              (lambda-ctx/init)
              (lambda/init-filters))

          ctx
          (warm-up-ctx preloaded)]
      (binding [*dal-state* (atom {:realm :warmup
                                   :realms {:warmup {}}})
                *queues* {:command-queue (atom [])
                          :seed 0}
                util/*cache* init-cache
                request/*request* (atom {:scoped true})
                log-state/*invocation-start-ns* (System/nanoTime)]
        (edd/with-stores ctx
          #(el-cmd/handle-commands
            %
            {:request-id     (uuid/gen)
             :interaction-id (uuid/gen)
             :commands       [{:cmd-id :warmup-cmd
                               :id     (uuid/gen)}]})))
      (log/info "SnapStart warm-up complete."))
    (catch Exception e
      (log/warn "SnapStart warm-up failed (non-fatal):" (ex-message e)))))

(defn- write-response! [output ^String payload]
  (with-open [o (io/writer output)]
    (.write o payload)))

(defn- on-success [_ctx response output]
  (log/info "OnSuccessFn writing success")
  (write-response! output (util/to-json response)))

(defn- on-error [ctx response output]
  (let [from-api?
        (boolean (or (:from-api ctx) (:statusCode response)))

        json
        (util/to-json response)]
    (log/infof "OnErrorFn writing error (from-api? %s)" from-api?)
    (write-response! output json)
    (when-not from-api?
      (throw (RuntimeException. json)))))

(defn java-request-handler
  [init-ctx handler & {:keys [filters post-filter]
                       :or   {filters     []
                              post-filter (fn [ctx] ctx)}}]
  (fn [_this input output ^Context lambda-context]
    ;; :scoped enables the per-invocation caches (see lambda.request/is-scoped).
    ;; Without it edd.el.query/with-cache short circuits and every remote
    ;; dependency is fetched over HTTP again, even when the resolved query is
    ;; identical. The custom runtime sets it in aws.lambda/lambda-custom-runtime.
    (binding [util/*cache* init-cache
              request/*request* (atom {:scoped true})
              log-state/*invocation-start-ns* (System/nanoTime)]
      (let [request
            {:body (util/to-edn (slurp input))}

            ctx
            (-> init-ctx
                (assoc :filters filters
                       :handler handler
                       :post-filter post-filter)
                (apply-cached-aws)
                (lambda-ctx/init)
                (aws-ctx/init)
                (lambda/init-filters)
                (assoc :from-api (lambda/is-from-api request)
                       :invocation-id (uuid/parse (.getAwsRequestId lambda-context))))]

        (ensure-metrics! ctx)
        (cache-aws! ctx)

        (lambda/send-response
         (lambda/handle-request ctx request)
         {:on-success-fn (fn [c r] (on-success c r output))
          :on-error-fn   (fn [c r] (on-error c r output))})))))

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

       (def ~'-handler-fn
         (edd.java-lambda-runtime.core/java-request-handler ~ctx ~handler ~@other))

       (def ~'-handleRequest
         (fn [~this-sym#
              ^java.io.InputStream ~input-stream-sym#
              ^java.io.OutputStream ~output-stream-sym#
              ^com.amazonaws.services.lambda.runtime.Context ~lambda-context-sym#]
           (~'-handler-fn ~this-sym# ~input-stream-sym# ~output-stream-sym# ~lambda-context-sym#)))

       ;; Runs at build time; its cost is captured in the snapshot, not on the
       ;; restore path. Warm up to load/JIT the request path into the snapshot,
       ;; drop credentials so the snapshot never carries stale ones, then GC to
       ;; shrink the snapshot (smaller snapshot => faster restore).
       (def ~'-beforeCheckpoint
         (fn [~this-sym# ^org.crac.Context ~crac-context-sym#]
           (let [start# (System/nanoTime)]
             (reset! edd.java-lambda-runtime.core/restore-ctx ~ctx)
             (edd.java-lambda-runtime.core/run-warm-up! ~ctx)
             (swap! edd.java-lambda-runtime.core/init-cache dissoc
                    :aws :aws-ctx-initialized)
             (System/gc)
             (clojure.tools.logging/info
              (format "lambda.Handler -beforeCheckpoint done in %.1f ms"
                      (edd.java-lambda-runtime.core/ms-since start#))))))

       ;; On the restore-critical path: keep it minimal. The metrics thread did
       ;; not survive the snapshot, and the snapshot credentials are stale.
       (def ~'-afterRestore
         (fn [~this-sym# ^org.crac.Context ~crac-context-sym#]
           (let [start# (System/nanoTime)]
             (swap! edd.java-lambda-runtime.core/init-cache dissoc :metrics-started)
             (try
               (let [fresh#
                     (aws.ctx/init @edd.java-lambda-runtime.core/restore-ctx)]
                 (swap! edd.java-lambda-runtime.core/init-cache assoc
                        :aws (:aws fresh#)
                        :aws-ctx-initialized true))
               (catch Exception e#
                 (swap! edd.java-lambda-runtime.core/init-cache dissoc
                        :aws :aws-ctx-initialized)
                 (clojure.tools.logging/warn
                  "lambda.Handler -afterRestore: credential refresh failed, refreshing on first invocation:"
                  (ex-message e#))))
             (clojure.tools.logging/info
              (format "lambda.Handler -afterRestore done in %.1f ms"
                      (edd.java-lambda-runtime.core/ms-since start#)))))))))
