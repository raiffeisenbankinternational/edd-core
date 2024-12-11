(ns lambda.logging
  (:require [jsonista.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging.impl]
            [lambda.request :as request]
            [clojure.stacktrace :as cst])
  (:import (java.time LocalDateTime)
           (org.slf4j LoggerFactory)
           (org.slf4j Logger)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- stacktrace-as-str
  [e]
  (if (instance? Throwable e)
    (with-out-str (cst/print-cause-trace e))
    "Not an exception"))

(defn- log-common-attrs
  [level msg]
  {:level     (str/upper-case (.getName ^clojure.lang.Keyword level))
   :message   msg
   :timestamp (LocalDateTime/now)
   :mdc       (if (bound? #'request/*request*)
                (get @request/*request*
                     :mdc {})
                {})})

(defn- log-structure
  ([level msg]
   (log-common-attrs level msg))
  ([level msg e]
   (assoc (log-common-attrs level msg) :error (stacktrace-as-str e))))

(defn- wrapped-json-for-aws
  [obj]
  (json/write-value-as-string obj))

(defn as-json
  (^String [level msg]
   (wrapped-json-for-aws (log-structure level msg)))
  (^String [level msg e]
   (wrapped-json-for-aws (log-structure level msg e))))

(defn slf4j-json-factory
  "Returns a SLF4J-based implementation of the LoggerFactory protocol, or nil if
  not available."
  []
  (let [; Same as is done inside LoggerFactory/getLogger(String).
        factory# (LoggerFactory/getILoggerFactory)]
    (extend Logger
      clojure.tools.logging.impl/Logger
      {:enabled?
       (fn [^Logger logger# level#]
         (if (= level#
                (and (bound? #'request/*request*)
                     (get-in @request/*request* [:mdc :log-level])
                     (-> (get-in @request/*request* [:mdc :log-level])
                         (name)
                         (str/lower-case)
                         (keyword))))
           true
           (condp = level#
             :trace (.isTraceEnabled logger#)
             :debug (.isDebugEnabled logger#)
             :info (.isInfoEnabled logger#)
             :warn (.isWarnEnabled logger#)
             :error (.isErrorEnabled logger#)
             :fatal (.isErrorEnabled logger#)
             (throw (IllegalArgumentException. (str level#))))))
       :write!
       (fn [^Logger logger# level# ^Throwable e# msg#]
         (let [^String msg# (if e#
                              (as-json level# msg# e#)
                              (as-json level# msg#))]
           "We don't care much about log level here
            because it can be overriden"
           (.info logger# msg#)))})
    (reify clojure.tools.logging.impl/LoggerFactory
      (name [_#]
        "org.slf4j")
      (get-logger [_# logger-ns#]
        (.getLogger factory# ^String (str logger-ns#))))))
