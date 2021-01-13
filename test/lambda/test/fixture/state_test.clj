(ns lambda.test.fixture.state-test
  (:require [clojure.test :refer :all]
            [lambda.test.fixture.state :as state]
            [lambda.uuid :as uuid]))

(def uuid1 (uuid/gen))
(def uuid2 (uuid/gen))
(def uuid3 (uuid/gen))

(deftest test-input
  (state/with-state
    (with-redefs [uuid/gen (fn []
                             (state/pop-item
                              :uuid/gen
                              [uuid3 uuid2 uuid1]))]
      (is (= uuid1
              (uuid/gen)))
      (is (= uuid2
              (uuid/gen)))
      (is (= uuid3
              (uuid/gen)))
      (is (= nil
              (uuid/gen))))))
