(ns lambda.test.fixture.core
  (:require [aws.lambda :as core]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [lambda.test.fixture.client :as client]
            [lambda.emf :as emf]))

(def region "eu-central-1")

(def next-url "http://mock/2018-06-01/runtime/invocation/next")

(defn get-env-mock
  [base e & [def]]
  (let [sysenv (System/getenv)
        real-val (get sysenv e)
        defaults {"AWS_ACCESS_KEY_ID"       ""
                  "AWS_SECRET_ACCESS_KEY"   ""
                  "AWS_LAMBDA_RUNTIME_API"  "mock"
                  "Region"                  region
                  "ResourceName"            "local-test"
                  "PublicHostedZoneName"    "example.com"
                  "EnvironmentNameLower"    "local"}
        env (merge defaults base)]
    (if (and real-val (not= real-val ""))
      real-val
      (get env e def))))

; TODO Update JWT tokens for this to work properl
(defn realm-mock
  [_ _ _] :test)

(defmacro mock-core
  [& args]
  (let [pairs (take-while #(keyword? (first %)) (partition 2 args))
        opts (into {} (map vec pairs))
        body (drop (* 2 (count pairs)) args)
        {:keys [invocations requests env] :or {env {}}} opts]
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
                     emf/start-metrics-publishing! (fn [] (constantly nil))
                     core/get-loop (fn [] (range 0 (count ~invocations)))
                     util/get-env (partial get-env-mock ~env)
                     util/get-current-time-ms (fn [] 1587403965)]
         (client/mock-http
          responses#
          ~@body)))))


