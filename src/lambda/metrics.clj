(ns lambda.metrics
  (:require
   [clojure.tools.logging :as log]
   [lambda.util :as util])
  (:import
   [java.lang.management ManagementFactory GarbageCollectorMXBean]
   [java.util Comparator Iterator Collections ArrayList Timer TimerTask]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^:private TEN_SECONDS 10000)

(def ^:private runtime-memory-denominator 1000000)

;; ------------------
;; GC collector discovery
;; ------------------

(def selected-gcs
  {;; GraalVM native-image non G1 GC
   "young generation scavenger"   "GCYoung"
   "complete scavenger"           "GCOldGen"

   ;; Java G1 GC
   "G1 Young Generation"          "GCYoung"
   "G1 Old Generation"            "GCOldGen"
   "G1 Concurrent GC"             "GCConcurrent"

   ;; Java Serial GC (used by Lambda SnapStart / Java 25)
   "Copy"                         "GCYoung"
   "MarkSweepCompact"             "GCOldGen"})

(def ^:private gcs-p
  (promise))

(def ^:private ^Comparator mx-gc-beans-comparator
  (comparator
   (fn [^GarbageCollectorMXBean o1 ^GarbageCollectorMXBean o2]
     (compare (.getName o1) (.getName o2)))))

(defn get-gc-collectors!
  []
  (if-not (realized? gcs-p)
    (locking ManagementFactory
      @(deliver
        gcs-p
        (let [mx-gc-beans (ArrayList. (ManagementFactory/getGarbageCollectorMXBeans))
              _ (Collections/sort mx-gc-beans mx-gc-beans-comparator)
              ^Iterator iter (.iterator mx-gc-beans)
              gcs (loop [gcs {}]
                    (if-not (.hasNext iter)
                      gcs
                      (let [^GarbageCollectorMXBean gc-bean (.next iter)
                            gc-name (.getName gc-bean)]
                        (log/info (format "GC collector: %s" gc-name))
                        (if-let [gc-as-name (get selected-gcs gc-name)]
                          (recur (assoc gcs gc-as-name gc-bean))
                          (recur gcs)))))
              found-gccs-by-names (set (keys gcs))]
          (when-not (or (contains? found-gccs-by-names "GCYoung")
                        (contains? found-gccs-by-names "GCOldGen"))
            (log/warnf "Unable to find GC collectors for metrics. Found raw collectors: %s"
                       (mapv #(.getName ^GarbageCollectorMXBean %) (ArrayList. (ManagementFactory/getGarbageCollectorMXBeans)))))
          gcs)))
    @gcs-p))

(defn init-metric-sources!
  "Eagerly initializes GC collector discovery. Call once at startup."
  []
  (log/info "Initializing metric sources...")
  (get-gc-collectors!)
  (log/info "Metric sources initialized.")
  nil)

;; ------------------
;; GC metrics collection
;; ------------------

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

(def ^:private last-gc-observations
  (atom {}))

(defn gc-metrics!
  "Collects current GC metrics as a map.
   Returns {:total-time N :total-count N :total-time-delta N :total-count-delta N}
   or nil when GC counters are all zero."
  []
  (let [collectors
        (get-gc-collectors!)

        totals
        (reduce-kv
         (fn [acc _gc-name ^GarbageCollectorMXBean gc-bean]
           (let [t (gc-bean->time gc-bean)
                 c (gc-bean->col-count gc-bean)]
             (-> acc
                 (update :total-time + t)
                 (update :total-count + c))))
         {:total-time 0 :total-count 0}
         collectors)

        ^long total-time (:total-time totals)
        ^long total-count (:total-count totals)

        {^long last-total-time :total-time
         ^long last-total-count :total-count}
        @last-gc-observations]

    (swap! last-gc-observations assoc :total-time total-time :total-count total-count)
    (when (and (> total-time 0) (> total-count 0))
      {:total-time total-time
       :total-count total-count
       :total-time-delta (- total-time (long (or last-total-time 0)))
       :total-count-delta (- total-count (long (or last-total-count 0)))})))

;; ------------------
;; Memory metrics collection
;; ------------------

(defn memory-metrics
  "Collects current memory metrics as a map.
   Returns {:in-use-mb N :available-mb N :total-mb N}."
  []
  (let [total
        (long (/ (.. Runtime getRuntime totalMemory) ^long runtime-memory-denominator))

        available
        (long (/ (.. Runtime getRuntime freeMemory) ^long runtime-memory-denominator))

        in-use
        (- total available)]
    {:in-use-mb in-use
     :available-mb available
     :total-mb total}))

;; ------------------
;; Metrics snapshot
;; ------------------

(defn collect-snapshot
  "Collects a system metrics snapshot with memory and GC data."
  []
  {:memory (memory-metrics)
   :gc (gc-metrics!)})

;; ------------------
;; Default formatter
;; ------------------

(defn- default-metrics-formatter
  "Default formatter. Receives a snapshot and logs it via log/info."
  [snapshot]
  (let [{:keys [in-use-mb available-mb total-mb]}
        (:memory snapshot)

        gc
        (:gc snapshot)]
    (log/info (str "METRIC memory/in-use(MB): " in-use-mb
                   "; available(MB): " available-mb
                   "; total(MB): " total-mb
                   (when gc
                     (str "; gc-time-delta(ms): " (:total-time-delta gc)
                          "; gc-count-delta: " (:total-count-delta gc)))))))

;; ------------------
;; Timer
;; ------------------

(defn start-metrics-publishing!
  "Starts a background timer that collects a metrics snapshot every 10 seconds
   and passes it to the formatter from [:logging :metrics-formatter] in ctx.
   Falls back to default-metrics-formatter (plain log/info) when absent."
  [ctx]
  (when-not (util/get-env "AWS_LAMBDA_DISABLE_METRICS")
    (init-metric-sources!)
    (let [formatter
          (get-in ctx [:logging :metrics-formatter] default-metrics-formatter)

          ^Timer timer
          (Timer. true)

          ^TimerTask task
          (proxy [TimerTask] []
            (run []
              (try
                (let [snapshot
                      (collect-snapshot)]
                  (formatter snapshot))
                (catch Exception e
                  (log/warn e "Error in metrics formatter")))))]
      (log/info "Starting metrics publishing...")
      (.scheduleAtFixedRate timer task (long 0) ^long TEN_SECONDS)
      (fn cancel-timer! [] (.cancel timer)))))
