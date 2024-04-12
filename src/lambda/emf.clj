(ns lambda.emf
  (:require
   [malli.core :as m]
   [malli.util :as mu]
   [clojure.tools.logging :as log]
   [malli.error :as me])
  (:import
   [java.lang.management ManagementFactory GarbageCollectorMXBean]
   [java.util Comparator Iterator Collections Timer TimerTask ArrayList]))

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
;; NOTE: Requires >= Java17 GraalVM Native Image distribution
(def
  selected-gcs
  {;; GraalVM native-image non G1 GC
   "young generation scavenger"   "GCYoung"
   "complete scavenger"           "GCOldGen"

   ;; Java G1 GC
   "G1 Young Generation"          "GCYoung"
   "G1 Old Generation"            "GCOldGen"
   "G1 Concurrent GC"             "GCConcurrent"})

(def ^:private gcs-p
  (promise))

(def ^:private ^Comparator mx-gc-beans-comparator
  (comparator
   (fn [^GarbageCollectorMXBean o1 ^GarbageCollectorMXBean o2]
     (compare (.getName o1) (.getName o2)))))

(defn- get-gc-collectors!
  []
  (if-not (realized? gcs-p)
    ;; Synchronize the access to prevent multiple threads from redelivering the promise
    (locking ManagementFactory
      @(deliver
        gcs-p
        (let [mx-gc-beans (ArrayList. (ManagementFactory/getGarbageCollectorMXBeans))
              ;; sort by name to ensure consistent ordering
              _ (Collections/sort mx-gc-beans mx-gc-beans-comparator)
              ^Iterator iter (.iterator mx-gc-beans)
              required-gcc-by-names (set (vals selected-gcs))
              gcs (loop [gcs {}]
                    (if-not (.hasNext iter)
                      gcs
                      (let [^GarbageCollectorMXBean gc-bean (.next iter)
                            gc-name (.getName gc-bean)]
                        (log/info (format "START: %s END" gc-name))
                        (if-let [gc-as-name (get selected-gcs gc-name)]
                          (recur (assoc gcs gc-as-name gc-bean))
                          (recur gcs)))))
              found-gccs-by-names (set (keys gcs))]
          (when-not (= required-gcc-by-names found-gccs-by-names)
            (log/warnf "Unable to find all required GC collectors. Required %s, found %s"
                       required-gcc-by-names found-gccs-by-names))
          gcs)))
    @gcs-p))

(defmacro pos-or-zero
  [expr]
  `(let [number# ~expr]
     (if (pos? number#)
       number#
       0)))

(defn- gc-bean->time ^long
  [^GarbageCollectorMXBean gc-bean]
  (pos-or-zero (-> gc-bean .getCollectionTime)))

(defn- gc-bean->col-count
  [^GarbageCollectorMXBean gc-bean]
  (pos-or-zero (-> gc-bean .getCollectionCount)))

(def ^:private gc-submetrics-directive-names
  ["Time" "Count"])

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

(defonce ^:private static-namespace-name "edd-embedded-metrics")

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
   [:= static-namespace-name]))

(def ^:private Dimension
  (m/schema
   [:or {}
    [:= "ServiceName"]
    [:= "ServiceType"]
    [:= "LogGroup"]]))

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
   #_GCMetricsTimeNames
   #_GCMetricsCountNames
   ["GCTotalTime" "GCTotalCount" "GCTotalTimeAggregated" "GCTotalCountAggregated"]))

(def ^:private GCMetricsCountNames+AggregatedCountNamesSet
  (set
   (concat
    GCMetricsCountNames
    ["GCTotalCount" "GCTotalCountAggregated"])))

(def ^:private AllMetricNames
  (vec
   (concat
    MemoryMetricsNames
    GCMetricsNames)))

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

    (throw (Exception. (format "Unknown unit for metric name %s" metric-name)))))

(def ^:private MetricDirective
  [:map {:closed true}
   ["Name" {} MetricDirectiveName]
   ["Unit" {} MetricDirectiveUnit]
   ["StorageResolution" {} [:int {:min 1 :max 60}]]])

(def ^:private MetricDirectiveWithValue
  (mu/merge
   MetricDirective
   [:map {:closed true}
    ["Value" {:optional true} :any]]))

(def ^:private AllMetrics
  (m/schema
   (apply
    conj
    [:tuple {}]
    (->>
     AllMetricNames
     (mapv
      (fn [metric-name]
        (mu/merge
         MetricDirective
         [:map {:closed true}
          ["Name" [:= metric-name]]
          ["Unit" [:= (metric-name->unit metric-name)]]])))))))

(defn ^:private metric-schema-by-name
  [metric-name]
  (condp contains? metric-name
    (set MemoryMetricsNames)
    [:int {:min 0}]

    (set GCMetricsNames)
    [:int {:min 0}]

    (throw (Exception. (format "Unknown validator for metric name %s" metric-name)))))

(def ^:private CloudWatchMetric
  (m/schema
   [:map {:closed true}
    ["Namespace" {} MetricNamespace]
    ["Dimensions" {} AllDimensions]
    ["Metrics" {} AllMetrics]]))

(def ^:private last-gc-observations
  (atom {}))

(defn gc-metrics!
  []
  (let [gc-metrics (vec (for [subdimension-key gc-submetrics-directive-names
                              [gc-name gc-bean] (get-gc-collectors!)
                              :let [bean->dimension-value
                                    (cond
                                      (= "Time" subdimension-key)
                                      gc-bean->time

                                      (= "Count" subdimension-key)
                                      gc-bean->col-count

                                      :else
                                      (throw (Exception. (format "Unknown subdimension key: %s" subdimension-key))))
                                    metric-name (str gc-name subdimension-key)]]
                          {"Name" metric-name
                           "Unit" (metric-name->unit metric-name)
                           "Value" (bean->dimension-value gc-bean)
                           "Subdimension" subdimension-key
                           "StorageResolution" 60}))
        {:strs [Time Count]} (group-by #(get % "Subdimension") gc-metrics)
        ^long total-time (reduce + (mapv #(get % "Value" 0) Time))
        ^long total-gc-count (reduce + (mapv #(get % "Value" 0) Count))
        {last-total-time :total-time
         last-total-count :total-gc-count} @last-gc-observations]
    (swap! last-gc-observations assoc :total-time total-time :total-gc-count total-gc-count)
    (when (and (> total-time 0) (> total-gc-count 0))
      ;; GC1 garbage collector is not yet supported in GraalVM native-image thus we will be getting 0 values all the time.
      ;; It's not worth to report those figures to CloudWatch
      [{"Name" "GCTotalTime"
        "Unit" "Milliseconds"
        "Value" (- total-time (long (or last-total-time 0)))
        "StorageResolution" 60}
       {"Name" "GCTotalCount"
        "Unit" "Count"
        "Value" (- total-gc-count (long (or last-total-count 0)))
        "StorageResolution" 60}
       {"Name" "GCTotalTimeAggregated"
        "Unit" "Milliseconds"
        "Value" total-time
        "StorageResolution" 60}
       {"Name" "GCTotalCountAggregated"
        "Unit" "Count"
        "Value" total-gc-count
        "StorageResolution" 60}])))

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
    (mapv #(-> [% (metric-schema-by-name %)]) (m/children MetricDirectiveName)))))

;; For non Lambda environments we don't want to throw an exception
(defn- ->function-name
  []
  (or (System/getenv "AWS_LAMBDA_FUNCTION_NAME") "unknownFunction"))

(defn- ->function-version
  []
  (or (System/getenv "AWS_LAMBDA_FUNCTION_VERSION") "$LATEST"))

(defn- ->log-group
  []
  (or (System/getenv "AWS_LAMBDA_LOG_GROUP_NAME") "unknownLogGroup"))

(defn- ->log-stream-id
  []
  (or (System/getenv "AWS_LAMBDA_LOG_STREAM_NAME") "123456"))

(defn- ->execution-environment
  []
  (or (System/getenv "AWS_EXECUTION_ENV") "UNKNOWN"))

(def ^:private runtime-memory-denominator 1000000)

(defn- total-memory
  []
  (long (/ (.. Runtime getRuntime totalMemory) ^long runtime-memory-denominator)))

(defn- free-memory
  []
  (long (/ (.. Runtime getRuntime freeMemory) ^long runtime-memory-denominator)))

(defn- memory-metrics
  {:malli/schema [:=> [] [:vector {} MetricDirectiveWithValue]]}
  []
  (let [^long total-memory (total-memory)
        ^long available-memory (free-memory)
        memory-in-use (- total-memory available-memory)]
    [{"Name"              "MemoryInUse"
      "Value"             memory-in-use
      "Unit"              "Megabytes"
      "StorageResolution" 60}
     {"Name"              "MemoryAvailable"
      "Unit"              "Megabytes"
      "Value"             available-memory
      "StorageResolution" 60}
     {"Name"              "MemoryTotal"
      "Unit"              "Megabytes"
      "Value"             total-memory
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

(def UserMetrics
  (m/schema
   [:vector {}
    [:map {:closed true}
     ["Name" {} :string]
     ["Unit" {} MetricDirectiveUnit]
     ["Value" {} [:or :int :double :string]]
     ["StorageResolution" {} [:int {:min 1 :max 60}]]]]))

(defn ->event
  {:malli/schema [:=> [:cat [:or UserMetrics :nil]] :map]}
  ([]
   (->event nil))
  ([user-metrics]
   (let [memory-metrics (memory-metrics)
         gc-metrics     (gc-metrics!)]
     (merge-event-with-metrics
      {"_aws"
       {"CloudWatchMetrics"
        [{"Namespace"  static-namespace-name
          "Dimensions" [#{"ServiceName" "ServiceType" "LogGroup"}]
          "Metrics"    []}]
        "Timestamp"         (now-timestamp)}
       "LogGroup"             (->log-group)
       "executionEnvironment" (->execution-environment)
       "ServiceType"          "AWS::Lambda::Function"
       "logStreamId"          (->log-stream-id)
       "functionVersion"      (->function-version)
       "ServiceName"          (->function-name)}
      (vec (concat memory-metrics gc-metrics user-metrics))))))

(def ^:private TEN_SECONDS
  "Timer should run every 10 seconds"
  10000)

(defn publish-event!
  {:malli/schema [:=> [:cat :map] :nil]}
  [event]
  ;; Lock on System/out to prevent interleaving of the output
  (locking System/out
    (.flush System/out)
    (. System/out println (to-json event))
    (.flush System/out)))

(defn- init-stateful-metric-sources!
  []
  (log/info "Intializing EMF stateful metric sources...")
  (get-gc-collectors!)
  (log/info "Finalized initialization of EMF stateful metric sources...")
  nil)

(defn start-metrics-publishing!
  []
  (log/info "Starting EMF Metrics publishing...")
  (init-stateful-metric-sources!)
  (if-not (deref gcs-p TEN_SECONDS nil)
    (log/error "Unable to retrieve GC collectors, metrics publishing will not be started!")
    (let [^Timer timer (Timer. true)
          ^TimerTask task (proxy [TimerTask] []
                            (run []
                              (let [event (->event)]
                                (publish-event! event))))]
      (.scheduleAtFixedRate timer task (long 0) ^long TEN_SECONDS)
      (fn cancel-timer! [] (.cancel timer)))))

(comment
  (require '[malli.error :as me])
  (require '[malli.dev :as dev])
  (dev/start!)
  (-> (m/explain AWSEvent (->event))
      (me/humanize)))

