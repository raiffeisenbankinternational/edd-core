(ns router.main
  (:gen-class)
  (:require [edd.java-lambda-runtime.core :as runtime]
            [edd.router.core :as router]
            [lambda.filters :as filters]))

(runtime/start
 {}
 router/handler
 :filters [filters/from-bucket])
