(ns lambda.http-client
  (:require [clojure.tools.logging :as log]
            [edd.util :as edd-util]))

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
  (let [attempt (- retry-count n)]
    (assoc
     req
     :connect-timeout (-> attempt
                          (* attempt)
                          (* connect-timeout-step)
                          (+ connect-timeout))
     :idle-timeout (-> attempt
                       (* attempt)
                       (* idle-timeout-step)
                       (+ idle-timeout)))))

(defn retry-n-impl
  "Retry function with 1 that accepts retry count"
  [f attempt total resp meta]
  (if (zero? attempt)
    (throw (ex-info (str "Failed to execute request: " attempt "/" total)
                    (merge {:total-attempts total
                            :error          resp}
                           meta)))
    (let [response (try
                     (apply f [attempt])
                     (catch Exception e
                       (edd-util/try-parse-exception-data e)))]
      (if (:error response)
        (do
          (log/warn (str "Retrying " (- total attempt) "/" total) (:error response))
          (when (not= attempt total)
            ;sleep only when second attempt
            (Thread/sleep (+ 1000 (rand-int 1000))))
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