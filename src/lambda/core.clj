(ns lambda.core
  (:gen-class)
  (:require [lambda.util :as util]
            [clojure.tools.logging :as log]
            [lambda.logging :as lambda-logging]
            [aws.lambda :as lambda]
            [clojure.string :as str]))

(defn start
  [& params]
  (alter-var-root #'clojure.tools.logging/*logger-factory*
                  (fn [f] (lambda-logging/slf4j-json-factory)))
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
      (apply lambda/lambda-custom-runtime params))))
