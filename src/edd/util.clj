(ns edd.util)

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

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

