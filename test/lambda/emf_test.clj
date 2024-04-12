(ns lambda.emf-test
  (:require
   [lambda.emf :as emf]
   [clojure.test :refer [deftest testing is]]
   [malli.core :as m]
   [malli.error :as me]))

(deftest emf-test
  (testing "AWS Embedded Metric Event should conform to the specification"
    (is (= nil (me/humanize (m/explain emf/AWSEvent (emf/->event))))))
  (testing "Publishing of custom event"
    (let [event (emf/->event [{"Name" "FacilitiesCount"
                               "Value" 30000
                               "Unit" "Count"
                               "StorageResolution" 60}])]
      (is (= 30000 (get event "FacilitiesCount")))
      (is (= [{"Name" "MemoryInUse",
               "Unit" "Megabytes",
               "StorageResolution" 60}
              {"Name" "MemoryAvailable",
               "Unit" "Megabytes",
               "StorageResolution" 60}
              {"Name" "MemoryTotal",
               "Unit" "Megabytes",
               "StorageResolution" 60}
              {"Name" "GCTotalTime",
               "Unit" "Milliseconds",
               "StorageResolution" 60}
              {"Name" "GCTotalCount",
               "Unit" "Count",
               "StorageResolution" 60}
              {"Name" "GCTotalTimeAggregated",
               "Unit" "Milliseconds",
               "StorageResolution" 60}
              {"Name" "GCTotalCountAggregated",
               "Unit" "Count",
               "StorageResolution" 60}
              {"Name" "FacilitiesCount",
               "Unit" "Count",
               "StorageResolution" 60}]
             (get-in event ["_aws" "CloudWatchMetrics" 0 "Metrics"]))))))
