(ns sdk.aws.common
  (:require
   [clj-aws-sign.core :as awssign]
   [clojure.tools.logging :as log])

  (:import (java.time.format DateTimeFormatter)
           (java.time OffsetDateTime ZoneOffset)))

(defn retry [f n & [resp]]
  (if (zero? n)
    (do (log/error "Retry failed" resp)
        (throw (RuntimeException. "Failed to execute request")))
    (let [response (f)]
      (if (:error response)
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry f (dec n) response))
        response))))

(defn create-date
  []
  (.format
   (. DateTimeFormatter ofPattern "yyyyMMdd'T'HHmmss'Z'")
   (. OffsetDateTime now (. ZoneOffset UTC))))

(defn authorize
  [req]
  (awssign/authorize req))
