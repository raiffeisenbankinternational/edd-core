(ns lambda.test.fixture.core
  (:require [clojure.test :refer :all]
            [lambda.core :as core]
            [lambda.util :as utils]
            [lambda.test.fixture.client :as client]))

(def region "eu-west-1")

(def env
  {"AWS_LAMBDA_RUNTIME_API" "mock"
   "Region"                 region})

(def next-url "http://mock/2018-06-01/runtime/invocation/next")

(defn get-env-mock
  [e & [def]]
  (get env e def))

(defmacro mock-core
  [& {:keys [invocations requests] :as body}]
  `(let [req-calls#
         (map-indexed
          (fn [idx# itm#]
            {:get     next-url
             :body    itm#
             :headers {:lambda-runtime-aws-request-id idx#}})
          ~invocations)
         responses# (vec (concat req-calls# ~requests))]
     (with-redefs [core/get-loop (fn [] (range 0 (count ~invocations)))
                   utils/get-env get-env-mock
                   utils/get-current-time-ms (fn [] 1587403965)]
       (client/mock-http
        responses#
        ~@body))))


