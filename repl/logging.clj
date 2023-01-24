(ns logging
  (:require [clojure.string :as str]
            [lambda.request :as request]
            [clojure.stacktrace :as cst]
            [clojure.tools.logging.impl :as log-impl])
  (:import (java.time LocalDateTime)
           (org.slf4j LoggerFactory)
           (org.slf4j Logger)))

(defn write-log
  [^Logger logger level ^Throwable e msg]
  (let [out msg]
    (if e
      (.error logger out e)
      (.info logger out))))

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
       :write! write-log})
    (reify clojure.tools.logging.impl/LoggerFactory
      (name [_#]
        "org.slf4j")
      (get-logger [_# logger-ns#]
        (.getLogger factory# ^String (str logger-ns#))))))

(alter-var-root #'clojure.tools.logging/*logger-factory*
                (fn [f] (logging/slf4j-json-factory)))

(defn print-fn
  [& xs]
  (str/join
   "\n"
   (map clojure.pprint/pprint xs)))

(alter-var-root #'clojure.core/print-str print-fn)
