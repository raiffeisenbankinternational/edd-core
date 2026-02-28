(ns edd.aws-emf.core
  (:require
   [lambda.metrics :as metrics]
   [malli.core :as m]
   [malli.util :as mu]
   [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;; ------------------
;; Patches to prevent cyclic-load dependency
(def to-json
  (do
    (require 'lambda.util)
    (resolve 'lambda.util/to-json)))
;; ------------------

;; ------------------
;; ------------------
;; Embedded Metric Format
;; Specification: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html
;; ------------------

;; Since we are using Lambda the CloudWatch agent is directly embedded, as a result
;; we can use simple println to send metrics to CloudWatch.
;; Be careful when configuring your metric extraction as it impacts your custom metric usage and corresponding bill.
;; If you unintentionally create metrics based on high-cardinality dimensions (such as requestId), the embedded metric format will by design create a custom metric corresponding to each unique dimension combination. For more information, see Dimensions.
;; As a result we want to limit the dimension to functionName (we are not using versioned Lambdas)

;; Implementation based on: https://github.com/awslabs/aws-embedded-metrics-java/

(defn- now-timestamp ^long
  []
  (System/currentTimeMillis))

;; For schema validation we need to use a static timestamp
(defonce ^:private static-lambda-runtime-timestamp
  (now-timestamp))

(defonce ^:private jvm-namespace "jvm")
(defonce ^:private edd-namespace "edd")

;; ------------------
;; Schemas
;; ------------------

(def Timestamp
  (m/-simple-schema
   {:type :timestamp
    :pred #(and (instance? Long %) (> (long %) (long static-lambda-runtime-timestamp)))
    :type-properties
    {:error/message "should be valid timestamp (number of milliseconds after Jan 1, 1970 00:00:00 UTC)"
     :gen/elements [(+ (now-timestamp) 1000)]}}))

(def ^:private MetricNamespace
  (m/schema
   [:or {}
    [:= jvm-namespace]
    [:= edd-namespace]]))

(def ^:private Dimension
  (m/schema
   [:or {}
    [:= "ServiceName"]
    [:= "ServiceType"]
    [:= "Type"]
    [:= "Operation"]
    [:= "Status"]]))

(def ^:private AllDimensions
  (m/schema
   [:vector {}
    [:set {}
     [:and :string Dimension]]]))

(def ^:private MetricDirectiveUnit
  (m/schema
   [:enum {}
    "Seconds" "Microseconds" "Milliseconds" "Bytes" "Kilobytes" "Megabytes" "Count"]))

(def ^:private MemoryMetrics
  (m/schema
   [:enum {:closed true}
    "MemoryInUse" "MemoryAvailable" "MemoryTotal"]))

(def ^:private MemoryMetricsNames
  (m/children MemoryMetrics))

(def ^:private GCMetricsTime
  (m/schema
   [:enum {:closed true}
    "GCOldGenTime" "GCConcurrentTime" "GCYoungTime"]))

(def ^:private GCMetricsTimeNames
  (m/children GCMetricsTime))

(def ^:private GCMetricsCount
  (m/schema
   [:enum {:closed true}
    "GCOldGenCount" "GCConcurrentCount" "GCYoungCount"]))

(def ^:private GCMetricsTimeNames+AggregatedTimeNamesSet
  (set
   (concat
    GCMetricsTimeNames
    ["GCTotalTime" "GCTotalTimeAggregated"])))

(def ^:private GCMetricsCountNames
  (m/children GCMetricsCount))

(def ^:private GCMetricsNames
  (concat
   ["GCTotalTime" "GCTotalCount" "GCTotalTimeAggregated" "GCTotalCountAggregated"]))

(def ^:private GCMetricsCountNames+AggregatedCountNamesSet
  (set
   (concat
    GCMetricsCountNames
    ["GCTotalCount" "GCTotalCountAggregated"])))

(def ^:private DurationMetricNames
  ["Duration"])

(def ^:private DurationMetricNamesSet
  (set DurationMetricNames))

(def ^:private AllMetricNames
  (vec
   (concat
    MemoryMetricsNames
    GCMetricsNames
    DurationMetricNames)))

(def ^:private MetricDirectiveName
  (m/schema
   (apply
    conj
    [:enum {:closed true}]
    AllMetricNames)))

(defn ^:private metric-name->unit
  [metric-name]
  (condp contains? metric-name
    (set MemoryMetricsNames)
    "Megabytes"

    GCMetricsTimeNames+AggregatedTimeNamesSet
    "Milliseconds"

    GCMetricsCountNames+AggregatedCountNamesSet
    "Count"

    DurationMetricNamesSet
    "Milliseconds"

    (throw (Exception. (format "Unknown unit for metric name %s" metric-name)))))

(def ^:private MetricDirective
  [:map {:closed true}
   ["Name" {} MetricDirectiveName]
   ["Unit" {} MetricDirectiveUnit]
   ["StorageResolution" {} [:int {:min 1 :max 60}]]])

(def ^:private AllMetrics
  (m/schema
   [:vector {}
    (apply
     conj
     [:or {}]
     (->>
      AllMetricNames
      (mapv
       (fn [metric-name]
         (mu/merge
          MetricDirective
          [:map {:closed true}
           ["Name" [:= metric-name]]
           ["Unit" [:= (metric-name->unit metric-name)]]])))))]))

(defn ^:private metric-schema-by-name
  [metric-name]
  (condp contains? metric-name
    (set MemoryMetricsNames)
    [:int {:min 0}]

    (set GCMetricsNames)
    [:int {:min 0}]

    DurationMetricNamesSet
    [:or :int :double]

    (throw (Exception. (format "Unknown validator for metric name %s" metric-name)))))

(def ^:private CloudWatchMetric
  (m/schema
   [:map {:closed true}
    ["Namespace" {} MetricNamespace]
    ["Dimensions" {} AllDimensions]
    ["Metrics" {} AllMetrics]]))

(def AWSEvent
  (m/schema
   (apply
    conj
    [:map {}
     ["_aws" {:closed true}
      [:map {:closed true}
       ["CloudWatchMetrics" {:closed true}
        [:vector {} CloudWatchMetric]]
       ["Timestamp" {} Timestamp]]]]
    (mapv #(-> [% {:optional true} (metric-schema-by-name %)]) (m/children MetricDirectiveName)))))

;; For non Lambda environments we don't want to throw an exception
(defn- ->function-name
  []
  (or (System/getenv "AWS_LAMBDA_FUNCTION_NAME") "unknownFunction"))

(defn- ->function-version
  []
  (or (System/getenv "AWS_LAMBDA_FUNCTION_VERSION") "$LATEST"))

(defn- ->log-stream-id
  []
  (or (System/getenv "AWS_LAMBDA_LOG_STREAM_NAME") "123456"))

(defn- ->execution-environment
  []
  (or (System/getenv "AWS_EXECUTION_ENV") "UNKNOWN"))

;; ------------------
;; Snapshot → EMF conversion
;; ------------------

(defn- snapshot->memory-metrics
  "Converts a lambda.metrics memory snapshot to EMF metric directives."
  [{:keys [in-use-mb available-mb total-mb]}]
  [{"Name"              "MemoryInUse"
    "Value"             in-use-mb
    "Unit"              "Megabytes"
    "StorageResolution" 60}
   {"Name"              "MemoryAvailable"
    "Unit"              "Megabytes"
    "Value"             available-mb
    "StorageResolution" 60}
   {"Name"              "MemoryTotal"
    "Unit"              "Megabytes"
    "Value"             total-mb
    "StorageResolution" 60}])

(defn- snapshot->gc-metrics
  "Converts a lambda.metrics GC snapshot to EMF metric directives.
   Returns nil when gc data is nil (no GC activity detected)."
  [gc]
  (when gc
    [{"Name" "GCTotalTime"
      "Unit" "Milliseconds"
      "Value" (:total-time-delta gc)
      "StorageResolution" 60}
     {"Name" "GCTotalCount"
      "Unit" "Count"
      "Value" (:total-count-delta gc)
      "StorageResolution" 60}
     {"Name" "GCTotalTimeAggregated"
      "Unit" "Milliseconds"
      "Value" (:total-time gc)
      "StorageResolution" 60}
     {"Name" "GCTotalCountAggregated"
      "Unit" "Count"
      "Value" (:total-count gc)
      "StorageResolution" 60}]))

(defn merge-event-with-metrics
  [event metrics]
  (reduce
   (fn [event {:strs [Name Value] :as metric}]
     (-> event
         (assoc Name Value)
         (update-in ["_aws" "CloudWatchMetrics" 0 "Metrics"] conj (dissoc metric "Value"))))
   event
   metrics))

(defn- ->base-event
  "Builds the common EMF event structure with the given CW namespace and dimensions."
  [cw-namespace dimensions]
  {"_aws"
   {"CloudWatchMetrics"
    [{"Namespace"  cw-namespace
      "Dimensions" dimensions
      "Metrics"    []}]
    "Timestamp"         (now-timestamp)}
   "executionEnvironment" (->execution-environment)
   "ServiceType"          "AWS::Lambda::Function"
   "logStreamId"          (->log-stream-id)
   "functionVersion"      (->function-version)
   "ServiceName"          (->function-name)})

(defn ->jvm-event
  "Builds an EMF event for background JVM metrics.
   CW Namespace: \"jvm\", Dimensions: [[\"ServiceName\"]]."
  [snapshot]
  (let [memory-metrics
        (snapshot->memory-metrics (:memory snapshot))

        gc-metrics
        (snapshot->gc-metrics (:gc snapshot))]
    (merge-event-with-metrics
     (->base-event jvm-namespace [#{"ServiceName"}])
     (vec (concat memory-metrics gc-metrics)))))

(defn ->edd-event
  "Builds an EMF event for per-request edd metrics (command/query).
   CW Namespace: \"edd\", Dimensions: [[\"ServiceName\" \"Type\" \"Operation\" \"Status\"]].
   metric-type  - string: \"command\" or \"query\"
   operation    - string: the command/query name, e.g. \"create-application\"
   snapshot     - {:memory {...} :gc {...}}
   duration-ms  - elapsed time in milliseconds
   status       - numeric status code (200, 520, 521, 522)"
  [metric-type operation snapshot duration-ms status]
  (let [memory-metrics
        (snapshot->memory-metrics (:memory snapshot))

        gc-metrics
        (snapshot->gc-metrics (:gc snapshot))

        duration-metric
        [{"Name" "Duration"
          "Value" duration-ms
          "Unit" "Milliseconds"
          "StorageResolution" 60}]]
    (merge-event-with-metrics
     (assoc (->base-event edd-namespace [#{"ServiceName" "Type" "Operation" "Status"}])
            "Type" metric-type
            "Operation" operation
            "Status" (str status))
     (vec (concat memory-metrics gc-metrics duration-metric)))))

(defn publish-event!
  {:malli/schema [:=> [:cat :map] :nil]}
  [event]
  ;; Lock on System/out to prevent interleaving of the output
  (locking System/out
    (.flush System/out)
    (. System/out println (to-json event))
    (.flush System/out)))

;; ------------------
;; Per-request metrics (d-metrics dispatch)
;; ------------------

(defn metric-label->type
  "Extracts the metric type from a label vector.
   e.g. [:query :get-order] -> \"query\""
  [label]
  (if (string? label)
    label
    (name (first label))))

(defn metric-label->operation
  "Extracts the operation name from a label vector.
   e.g. [:query :get-order]  -> \"get-order\"
        [:health]            -> \"health\""
  [label]
  (if (string? label)
    label
    (name (or (second label) (first label)))))

(defn publish-user-metric!
  "Publishes a per-request metric event via EMF. Accepts a simple metric map
   {:label [:command :create-application] :elapsed-ms 42.5 :offset-ms 123.4 :status 200}
   and emits an EMF event with CW Namespace \"edd\", dimensions
   ServiceName/Type/Operation/Status, and metrics Duration + JVM."
  [metric]
  (try
    (let [snapshot
          (metrics/collect-snapshot)

          metric-type
          (metric-label->type (:label metric))

          operation
          (metric-label->operation (:label metric))

          status
          (or (:status metric) 200)]
      (publish-event! (->edd-event metric-type operation snapshot (:elapsed-ms metric) status)))
    (catch Exception e
      (log/warn e "Failed to publish EMF metric event"))))

(defn- emf-metrics-formatter
  "Metrics formatter for the background timer in lambda.metrics.
   Receives a snapshot and publishes as a JVM EMF event."
  [snapshot]
  (publish-event! (->jvm-event snapshot)))

(defn register
  "Registers EMF metrics into the context.
   Stores the per-request metrics fn at [:logging :metrics]
   and the background metrics formatter at [:logging :metrics-formatter]."
  [ctx]
  (log/info "Registering AWS EMF metrics")
  (-> ctx
      (assoc-in [:logging :metrics] publish-user-metric!)
      (assoc-in [:logging :metrics-formatter] emf-metrics-formatter)))
