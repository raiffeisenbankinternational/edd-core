(ns lambda.emf-test
  (:require
   [lambda.emf :as emf]
   [clojure.test :refer [deftest testing is]]
   [malli.core :as m]
   [malli.error :as me]))

(deftest emf-test
  (testing "AWS Embedded Metric Event should conform to the specification"
    (is (= nil (me/humanize (m/explain emf/AWSEvent (emf/->event)))))))
