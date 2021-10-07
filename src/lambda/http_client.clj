(ns lambda.http-client
  (:require [clojure.tools.logging :as log]))

(def retry-count 3)
(defn request->with-timeouts
  "Assign timeouts based on retry attempt
  We take squre function of attempt and multiply"
  [n req & {:keys [connect-timeout
                   connect-timeout-step
                   idle-timeout
                   idle-timeout-step]
            :or {connect-timeout 200
                 connect-timeout-step 500
                 idle-timeout 5000
                 idle-timeout-step 4000}}]
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

(defn retry-n-impl [f attempt total & [resp]]
  "Retry function with 1 that accepts retry count"
  (if (zero? attempt)
    (do (log/error "Retry failed" resp)
        (throw (ex-info "Failed to execute request"
                        {:total-attempts total})))
    (let [response (f attempt)]
      (if (:error response)
        (do
          (log/warn "Retrying" (- total attempt))
          (when (not= attempt total)
            ;sleep only when second attempt
            (Thread/sleep (+ 1000 (rand-int 1000))))
          (retry-n-impl f (dec attempt) total response))
        response))))

(defn retry-n
  [function-to-retry & {:keys [retries]
                        :or {retries retry-count}}]

  "Retry function with 1 that accepts retry count"
  (retry-n-impl function-to-retry
                retries
                retries))