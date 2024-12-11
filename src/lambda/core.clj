(ns lambda.core
  (:gen-class)
  (:require [lambda.util :as util]
            [clojure.tools.logging :as log]
            [aws.lambda :as lambda]
            [clojure.string :as str]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn start
  [& params]
  (try
    (let [ctx (first params)
          startup-milis (Long/parseLong
                         (str
                          (util/get-property "edd.startup-milis" 0)))]
      (log/info "Server started: " (- (System/currentTimeMillis)
                                      startup-milis))
      (if-let [runtime-handler (or (:edd/runtime ctx)
                                   (System/getProperty "edd.runtime"))]
        (do
          (log/info "Custom runtime: " runtime-handler)
          (require (-> (str/split runtime-handler #"/")
                       (first)
                       (symbol)))
          (apply (resolve (symbol runtime-handler)) params))
        (apply lambda/lambda-custom-runtime params)))
    (catch Exception ex
      ;; log via println, since Init error can also come from the logger,
      ;; and we want to make sure we can see it
      (println "Error starting custom runtime. Potentially Init Phase error" ex)
      (throw ex))))
