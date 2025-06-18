(ns lambda.uuid-test
  (:require [clojure.test :refer :all]
            [lambda.uuid :as uuid]))

(deftest uuid-v3-test
  (is (= #uuid "3baf44d9-6cde-3bb0-89e0-059c66704ef7"
         (uuid/named "2021-12-27"))))
