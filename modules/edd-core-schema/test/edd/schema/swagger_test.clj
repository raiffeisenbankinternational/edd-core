(ns edd.schema.swagger-test
  (:require [glms-schema.swagger :as sut]
            [clojure.test :refer [deftest testing is]]))

(deftest test-generating-swagger-schema
  (testing "should have correct structure and use supplied options"
    (let [options {:service     "limit-lifecycle-svc"
                   :hostname    "glms-limit-lifecycle-svc.rbi.cloud"
                   :port        8443
                   :title       "Limit Lifecycle API"
                   :description "This API provides commands and queries..."
                   :version     "0.0.1"}
          swagger (sut/generate 'limit-lifecycle.schema options)]

      (is (seq swagger))
      (is (= (:service options)
             (:basePath swagger)))
      (is (= (str (:hostname options) ":" (:port options))
             (:host swagger)))
      (is (= (select-keys options [:title :description :version])
             (:info swagger)))

      (is (seq (get-in swagger [:paths "/command" :post :parameters 0 :schema :oneOf])))
      (is (seq (get-in swagger [:paths "/query" :post :parameters 0 :schema :oneOf]))))))
