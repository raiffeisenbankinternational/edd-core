(ns lambda.logging-local
  (:require [clojure.string :as str]
            [clojure.tools.logging.impl]
            [lambda.request :as request]
            [lambda.logging.state :as log-state]
            [clojure.stacktrace :as cst])
  (:import (org.slf4j LoggerFactory)
           (org.slf4j Logger)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- stacktrace-as-str
  [e]
  (if (instance? Throwable e)
    (with-out-str (cst/print-cause-trace e))
    "Not an exception"))

(defn- get-request-log-level
  "Extract log level from request context, returns nil if not found"
  []
  (when (bound? #'request/*request*)
    (some-> @request/*request*
            (get-in [:mdc :log-level])
            name
            str/lower-case
            keyword)))

(defn- log-common-attrs
  [level msg]
  (let [depth (if (bound? #'log-state/*d-time-depth*) log-state/*d-time-depth* 0)
        indent (str/join "" (repeat depth "  "))]
    (str
     indent
     (format "%-4s "
             (str/upper-case (.getName ^clojure.lang.Keyword level)))
     msg)))

(defn- log-structure
  ([level msg]
   (log-common-attrs level  msg))
  ([level msg e]
   (str
    (log-common-attrs level msg)
    (stacktrace-as-str e))))

(defn slf4j-local-factory
  "Returns a SLF4J-based implementation of the LoggerFactory protocol, or nil if
  not available."
  []
  (let [; Same as is done inside LoggerFactory/getLogger(String).
        factory# (LoggerFactory/getILoggerFactory)]
    (extend Logger
      clojure.tools.logging.impl/Logger
      {:enabled?
       (fn [^Logger logger# level#]
         (let [request-level (get-request-log-level)]
           (if (and request-level (= level# request-level))
             true
             (condp = level#
               :trace (.isTraceEnabled logger#)
               :debug (.isDebugEnabled logger#)
               :info (.isInfoEnabled logger#)
               :warn (.isWarnEnabled logger#)
               :error (.isErrorEnabled logger#)
               :fatal (.isErrorEnabled logger#)
               (throw (IllegalArgumentException. (str level#)))))))
       :write!
       (fn [^Logger logger# level# ^Throwable e# msg#]
         (let [^String msg# (if e#
                              (log-structure level# msg# e#)
                              (log-structure level# msg#))]
           ;; We don't care much about log level here
           ;; because it can be overridden
           (.info logger# msg#)))})
    (reify clojure.tools.logging.impl/LoggerFactory
      (name [_#]
        "org.slf4j")
      (get-logger [_# logger-ns#]
        (.getLogger factory# (str logger-ns#))))))
