(ns doc.edd.ctx
  (:require [clojure.test :refer :all]))

(comment "Context is a map used to contain system configuration,
          handler, request and response values"
         "It is usually passed to most of the function in ed-core
         and to some of the handlers.")

(def init-ctx {})

