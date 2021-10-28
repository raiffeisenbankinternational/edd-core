(ns lambda.core
  (:gen-class)
  (:require [lambda.util :as util]
            [clojure.tools.logging :as log]
            [aws.lambda :as lambda]
            [clojure.string :as str]))

(defn start
  [& params]
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
