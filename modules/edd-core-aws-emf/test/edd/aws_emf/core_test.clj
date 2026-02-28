(ns edd.aws-emf.core-test
  (:require
   [edd.aws-emf.core :as emf]
   [lambda.metrics :as metrics]
   [clojure.test :refer [deftest testing is]]
   [malli.core :as m]
   [malli.error :as me]))

(deftest jvm-event-test
  (testing "JVM event should conform to schema"
    (let [snapshot
          (metrics/collect-snapshot)

          event
          (emf/->jvm-event snapshot)]

      (is
       (= nil
          (me/humanize (m/explain emf/AWSEvent event))))

      (is
       (= "jvm"
          (get-in event ["_aws" "CloudWatchMetrics" 0 "Namespace"])))

      (is
       (= [#{"ServiceName"}]
          (get-in event ["_aws" "CloudWatchMetrics" 0 "Dimensions"]))))))

(deftest edd-event-test
  (testing "EDD command event should conform to schema"
    (let [snapshot
          (metrics/collect-snapshot)

          event
          (emf/->edd-event "command" "create-application" snapshot 42.5 200)]

      (is
       (= nil
          (me/humanize (m/explain emf/AWSEvent event))))

      (is
       (= "edd"
          (get-in event ["_aws" "CloudWatchMetrics" 0 "Namespace"])))

      (is
       (= [#{"ServiceName" "Type" "Operation" "Status"}]
          (get-in event ["_aws" "CloudWatchMetrics" 0 "Dimensions"])))

      (is
       (= "command"
          (get event "Type")))

      (is
       (= "create-application"
          (get event "Operation")))

      (is
       (= "200"
          (get event "Status")))

      (is
       (= 42.5
          (get event "Duration")))))

  (testing "EDD event includes JVM metrics alongside Duration"
    (let [snapshot
          (metrics/collect-snapshot)

          event
          (emf/->edd-event "query" "get-order" snapshot 8.3 200)

          metric-names
          (set (map #(get % "Name")
                    (get-in event ["_aws" "CloudWatchMetrics" 0 "Metrics"])))]

      (is
       (contains? metric-names "Duration"))

      (is
       (contains? metric-names "MemoryInUse"))

      (is
       (contains? metric-names "MemoryAvailable"))

      (is
       (contains? metric-names "MemoryTotal")))))

(deftest register-test
  (testing "register puts metrics fn into ctx"
    (let [ctx (emf/register {})]

      (is
       (fn?
        (get-in ctx [:logging :metrics])))

      (is
       (fn?
        (get-in ctx [:logging :metrics-formatter]))))))

(deftest metric-label->type-test
  (testing "extracts type from label vector"
    (is
     (= "command"
        (emf/metric-label->type [:command :create-application]))))

  (testing "extracts type from single-element vector"
    (is
     (= "health"
        (emf/metric-label->type [:health]))))

  (testing "string passthrough"
    (is
     (= "already-a-string"
        (emf/metric-label->type "already-a-string")))))

(deftest metric-label->operation-test
  (testing "extracts operation from label vector"
    (is
     (= "get-order"
        (emf/metric-label->operation [:query :get-order]))))

  (testing "single keyword vector falls back to first element"
    (is
     (= "health"
        (emf/metric-label->operation [:health]))))

  (testing "string passthrough"
    (is
     (= "already-a-string"
        (emf/metric-label->operation "already-a-string")))))
