(ns lambda.http-client
  (:require [clojure.tools.logging :as log]
            [edd.util :as edd-util]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def retry-count 5)

(defn request->with-timeouts
  "Assign timeouts based on retry attempt
  We take squre function of attempt and multiply"
  [n req & {:keys [connect-timeout
                   connect-timeout-step
                   idle-timeout
                   idle-timeout-step]
            :or   {connect-timeout      300
                   connect-timeout-step 100
                   idle-timeout         5000
                   idle-timeout-step    4000}}]
  (let [attempt (long (- (long retry-count) (long n)))]
    (assoc
     req
     :keepalive -1
     :connect-timeout (long (-> attempt
                                (* attempt)
                                (* (long connect-timeout-step))
                                (+ (long connect-timeout))))
     :idle-timeout (long (-> attempt
                             (* attempt)
                             (* (long idle-timeout-step))
                             (+ (long idle-timeout)))))))

(defn retry-n-impl
  "Retry function with 1 that accepts retry count"
  [f attempt total resp meta]
  (if (zero? (long attempt))
    (throw (ex-info (str "Failed to execute request: " attempt "/" total)
                    (merge {:total-attempts total
                            :error          resp}
                           meta)))
    (let [total (long total)
          attempt (long attempt)
          response (try
                     (apply f [attempt])
                     (catch Exception e
                       (when (= attempt 1)
                         (throw
                          (ex-info "All retries exhosted"
                                   (merge {:message "All retries exhosted"
                                           :total-attempts total}
                                          (ex-data e))
                                   e)))
                       (edd-util/try-parse-exception-data e)))]
      (if (or
           (:error response)
           (> (long (:status response 0)) 499))
        (do
          (log/warnf
           "Retrying %s/%s, because: %s"
           (- total attempt)
           total
           (or (:error response)
               (format "response status: %s, with body %s"
                       (:status response)
                       (:body response))))
          (when (not= attempt total)
            ;sleep only when second attempt
            (Thread/sleep (long (+ 1000 (long (rand-int 1000))))))
          (retry-n-impl f (dec attempt) total response meta))
        response))))

(defn retry-n
  "Retry function with 1 that accepts retry count"
  [function-to-retry & {:keys [retries meta]
                        :or   {retries retry-count
                               meta    {}}}]
  (retry-n-impl function-to-retry
                retries
                retries
                {}
                meta))
