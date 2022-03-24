(ns edd.util
  (:require [clojure.tools.logging :as log]))

(defn try-parse-exception-data
  [e]
  (let [data (ex-data e)]
    (if data
      (if (:error data)
        data
        {:error   e
         :message data})
      {:error   e
       :message (try (.getMessage e)
                     (catch IllegalArgumentException e
                       (log/error e)
                       e))})))

