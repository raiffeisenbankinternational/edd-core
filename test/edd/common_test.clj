(ns edd.common-test
  (:require [clojure.test :refer :all]
            [edd.common :as common]
            [lambda.uuid :as uuid])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-parse-param
  (is (= "simple-test"
         (common/parse-param "simple-test")))
  (is (= 45
         (common/parse-param 45)))
  (is (= "sa"
         (common/parse-param {:query-id ""
                              :id       "sa"})))

  (is (= 45
         (common/parse-param {:query-id ""
                              :sequence 45})))
  (is (thrown?
       ExceptionInfo
       (common/parse-param {:query-id ""
                            :sequence 45
                            :wong     :attr})))
  (let [id (uuid/gen)]
    (is (= id
           (common/parse-param {:query-id " "
                                :name     id})))
    (is (= id
           (common/parse-param id)))))