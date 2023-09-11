(ns edd.util)

(defn try-parse-exception-data
  [^Throwable e]
  (let [data (ex-data e)]
    (if data
      (if (:error data)
        data
        {:error   e
         :message data})
      {:error   e
       :message (ex-message e)})))

