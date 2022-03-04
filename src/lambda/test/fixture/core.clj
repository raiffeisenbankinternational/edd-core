(ns lambda.test.fixture.core
  (:require [clojure.test :refer :all]
            [aws.lambda :as core]
            [lambda.filters :as fl]
            [lambda.util :as utils]
            [lambda.test.fixture.client :as client]
            [lambda.util :as util]
            [clojure.tools.logging :as log]))

(def region "eu-central-1")

(def next-url "http://mock/2018-06-01/runtime/invocation/next")

(defn get-env-mock
  [base e & [def]]
  (let [sysenv (System/getenv)
        env (merge
             {"AWS_LAMBDA_RUNTIME_API" "mock"
              "Region"                 region}
             base)]
    (get env e (get sysenv e def))))

; TODO Update JWT tokens for this to work properl
(defn realm-mock
  [_ _ _] :test)

(defmacro mock-core
  [& {:keys [invocations requests env] :or {env {}} :as body}]
  `(let [req-calls#
         (map-indexed
          (fn [idx# itm#]
            {:get     next-url
             :body    itm#
             :headers {:lambda-runtime-aws-request-id (get (util/to-edn itm#)
                                                           :invocation-id
                                                           idx#)}})
          ~invocations)
         responses# (vec (concat req-calls# ~requests))]
     (with-redefs [fl/get-realm realm-mock
                   core/get-loop (fn [] (range 0 (count ~invocations)))
                   utils/get-env (partial get-env-mock ~env)
                   utils/get-current-time-ms (fn [] 1587403965)]
       (client/mock-http
        responses#
        ~@body))))


