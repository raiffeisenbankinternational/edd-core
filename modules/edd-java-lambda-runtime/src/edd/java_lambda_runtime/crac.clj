(ns edd.java-lambda-runtime.crac
  (:require
   [edd.core]
   [clojure.main]
   [clojure.core]
   [clojure.tools.logging :as log])
  (:import
   [org.crac Resource Core]))

(defn dummy-resource []
  (proxy [Resource] []
    (beforeCheckpoint [ctx]
      (log/info "[snapstart] Preparing to checkpoint execution"))
    (afterRestore [ctx]
      (log/info "[snapstart] Restoring execution from the context"))))

(defn dummy-registration
  []
  (let [resource (dummy-resource)]
    (.register (Core/getGlobalContext) resource)))

(dummy-registration)