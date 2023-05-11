(ns sdk.aws.xray-test
  (:require [clojure.test :refer [deftest testing is]]
            [sdk.aws.xray :as xray]))

(deftest test-parse-token
  (let [token-map {:Root "1-abcdef12-34567890abcdef012345678"
                   :Sampled "1"}
        token-string  "Root=1-abcdef12-34567890abcdef012345678;Sampled=1"]
    (is (= token-map
           (xray/parse-xray-token (str "  "
                                       token-string))))
    (is (= token-string
           (xray/render-xray-token token-map))))
  (let [token-map {:Other "foo"
                   :Root "1-abcdef12-34567890abcdef012345678"
                   :Sampled "1"}
        token-string "Other=foo;Root=1-abcdef12-34567890abcdef012345678;Sampled=1"]
    (is (= token-map
           (xray/parse-xray-token token-string)))
    (is (= token-string
           (xray/render-xray-token token-map)))))


