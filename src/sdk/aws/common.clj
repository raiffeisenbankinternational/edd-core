(ns sdk.aws.common
  (:require
   [clj-aws-sign.core :as awssign]
   [clojure.tools.logging :as log])

  (:import (java.time.format DateTimeFormatter)
           (java.time OffsetDateTime ZoneOffset)))

(def ^:dynamic retry-count)

(defn retry [f n & [resp]]
  (if (zero? n)
    (do (log/error "Retry failed" resp)
        (throw (ex-info "Failed to execute request" {:message "Failed to execute request"
                                                     :error   (:error resp)})))
    (let [{:keys [error] :as response} (f)]
      (if error
        (do (log/warn (str "Failed handling attempt: " n) error)
            (Thread/sleep (+ 1000 (rand-int 1000)))
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
