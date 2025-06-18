(ns sdk.aws.common
  (:require
   [clj-aws-sign.core :as awssign]
   [clojure.tools.logging :as log])

  (:import (java.time.format DateTimeFormatter)
           (java.time OffsetDateTime ZoneOffset)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic retry-count)

(defn retry [f n & [resp]]
  (let [n (long n)]
    (if (zero? n)
      (do (log/error "Retry failed" resp)
          (throw (ex-info "Failed to execute request" {:message "Failed to execute request"
                                                       :error   (:error resp)})))
      (let [{:keys [error] :as response} (f)]
        (if error
          (do (log/warn (str "Failed handling attempt: " n) error)
              (Thread/sleep (long (+ 1000 (long (rand-int 1000)))))
              (retry f (dec n) response))
          response)))))

(defmacro with-retry [[retries timeout] & body]
  (let [retries (or retries 3)
        timeout (or timeout 1000)]
    `(loop [n# ~retries]
       (let [[result# e#]
             (try
               [(do ~@body) nil]
               (catch Throwable e#
                 (log/errorf e# "Retry has failed, n: %s" n#)
                 [nil e#]))]
         (if e#
           (if (zero? n#)
             (throw (ex-info "All attempts have been exhaused"
                             {:retries ~retries
                              :timeout ~timeout}
                             e#))
             (let [timeout#
                   (long (+ ~timeout (rand-int ~timeout)))]
               (Thread/sleep timeout#)
               (recur (dec n#))))
           result#)))))

(defn create-date
  []
  (.format
   (. DateTimeFormatter ofPattern "yyyyMMdd'T'HHmmss'Z'")
   (. OffsetDateTime now (. ZoneOffset UTC))))

(defn authorize
  [req]
  (awssign/authorize req))
