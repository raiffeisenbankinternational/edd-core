(ns lambda.logging
  (:require [clojure.tools.logging.impl :refer [LoggerFactory Logger]]
            [jsonista.core :as json]
            [clojure.string :as str]
            [lambda.request :as mdc]
            [clojure.stacktrace :as cst])
  (:import (java.time LocalDateTime)))

(defn- stacktrace-as-str
  [e]
  (if (instance? java.lang.Throwable e)
    (with-out-str (cst/print-stack-trace e))
    "Not an exception"))

(defn- log-common-attrs
  [level msg]
  {:level     (str/upper-case (.getName level))
   :message   msg
   :timestamp (LocalDateTime/now)
   :mdc       (if (bound? #'mdc/*request*)
                (get @mdc/*request*
                     :mdc {})
                {})})

(defn- log-structure
  ([level msg]
   (log-common-attrs level msg))
  ([level msg e]
   (assoc (log-common-attrs level msg) :error (stacktrace-as-str e))))

(defn- wrapped-json-for-aws
  [obj]
  (str "/*" (json/write-value-as-string obj) "*/"))

(defn as-json
  (^String [level msg]
   (wrapped-json-for-aws (log-structure level msg)))
  (^String [level msg e]
   (wrapped-json-for-aws (log-structure level msg e))))

(defn to-lower
  [k]
  (keyword (clojure.string/lower-case (name k))))

(defn factory
  "Returns a java.util.logging-based implementation of the LoggerFactory protocol,
  or nil if not available."
  []
  (eval
   `(let [levels# {:trace java.util.logging.Level/FINEST
                   :debug java.util.logging.Level/FINE
                   :info  java.util.logging.Level/INFO
                   :warn  java.util.logging.Level/WARNING
                   :error java.util.logging.Level/SEVERE
                   :fatal java.util.logging.Level/SEVERE}]
      (extend java.util.logging.Logger
        Logger
        {:enabled?
         (fn [^java.util.logging.Logger logger# level#]
           (.isLoggable logger# (get levels# level# level#)))
         :write!
         (fn [^java.util.logging.Logger logger# level# ^Throwable e# msg#]
           (let [^clojure.lang.Keyword actual-level# (keyword (to-lower (if (bound? #'mdc/*request*)
                                                                          (get-in @mdc/*request* [:mdc :level] level#)
                                                                          level#)))
                 ^java.util.logging.Level jul-level# (get levels# actual-level# level#)
                 ^String msg# (str msg#)]
             (if e#
               (.log logger# jul-level# (as-json actual-level# msg# e#) e#)
               (.log logger# jul-level# (as-json actual-level# msg#)))))})
      (reify LoggerFactory
        (name [_#]
          "java.util.logging")
        (get-logger [_# logger-ns#]
          (java.util.logging.Logger/getLogger (str logger-ns#)))))))
