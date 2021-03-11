(ns lambda.test.fixture.core
  (:require [clojure.test :refer :all]
            [lambda.core :as core]
            [lambda.util :as utils]
            [lambda.test.fixture.client :as client]))

(def region "eu-west-1")

(def next-url "http://mock/2018-06-01/runtime/invocation/next")

(defn get-env-mock
  [base e & [def]]
  (get (merge {"AWS_LAMBDA_RUNTIME_API" "mock"
               "Region"                 region}
              base) e def))

(defmacro mock-core
  [& {:keys [invocations requests env] :or {env {}} :as body}]
  `(let [req-calls#
         (map-indexed
          (fn [idx# itm#]
            {:get     next-url
             :body    itm#
             :headers {:lambda-runtime-aws-request-id idx#}})
          ~invocations)
         responses# (vec (concat req-calls# ~requests))]
     (with-redefs [core/get-loop (fn [] (range 0 (count ~invocations)))
                   utils/get-env (partial get-env-mock ~env)
                   utils/get-current-time-ms (fn [] 1587403965)]
       (client/mock-http
        responses#
        ~@body))))


