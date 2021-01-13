(ns lambda.util-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [lambda.test.fixture.client :as client]
            [org.httpkit.client :as http])

  (:import (java.time OffsetDateTime)))

(deftest test-parser-deserialization
  (let [result (util/to-edn
                "{\"json\": true, \"converted\": { \"edn\": true, \"bla\" : \":ble\",\"blo\":\"::bli\", \"li\":[\"a\", \":a\",\"::a\"]}}")]
    (is (= result {:json true,
                   :converted
                   {:edn true,
                    :bla :ble,
                    :blo ":bli",
                    :li  ["a" :a ":a"]}}))))

(deftest test-parser-array-keys-serialization
  (let [result (util/to-edn
                "{\"request-id\":\"#780b4fdd-581f-4261-8d6f-09b92ef97c09\",\"interaction-id\":\"#6e97b6a3-1078-4e1f-a395-522d6a50e44a\",\"query\":{\"query-id\":\":list-risks-on\",\"search\":[\":fields\",[\"attrs.cocunut\"],\":value\",\"109\"]},\"user\":{\"selected-role\":\":lime-sem-experts\"}}")]
    (is (= {:interaction-id #uuid "6e97b6a3-1078-4e1f-a395-522d6a50e44a"
            :query          {:query-id :list-risks-on
                             :search   [:fields ["attrs.cocunut"]
                                        :value "109"]}
            :request-id     #uuid "780b4fdd-581f-4261-8d6f-09b92ef97c09"
            :user           {:selected-role :lime-sem-experts}}
           result))))

(deftest test-parser-serialization
  (let [result (util/to-json {:json true,
                              :converted
                              {:edn true,
                               :bla :ble,
                               :blo ":bli",
                               :li  ["a" :a ":a"]}})]
    (is (= result "{\"json\":true,\"converted\":{\"edn\":true,\"bla\":\":ble\",\"blo\":\"::bli\",\"li\":[\"a\",\":a\",\"::a\"]}}"))))

(deftest test-uuid-deserialization
  (let [result (util/to-edn
                "{\"json\": \"#c2baf5fb-1268-4a25-bd12-ece185e86104\"}")]
    (is (= result {:json (uuid/parse "c2baf5fb-1268-4a25-bd12-ece185e86104")}))))

(deftest test-double-serialization
  (is (= "{\"c\":\":d\",\"e\":\"{\\\"a\\\":\\\":b\\\"}\"}"
         (util/to-json
          {:c :d
           :e (util/to-json
               {:a :b})}))))

(deftest test-parser-serialization
  (let [result (util/to-json {:json (uuid/parse "c2baf5fb-1268-4a25-bd12-ece185e86104")})]
    (is (= result "{\"json\":\"#c2baf5fb-1268-4a25-bd12-ece185e86104\"}"))))

(deftest test-get-env
  (is (= (util/get-env "USER")
         (System/getenv "USER"))))

(deftest test-generate-current-offsetdate
  (let [result (util/date-time)]
    (is (= (type result) OffsetDateTime))))

(deftest test-generate-offsetdate
  (let [result (util/date-time "2020-03-03T11:24:47.473+01:00")]
    (is (= (type result) OffsetDateTime))
    (is (= (.getDayOfMonth result) 3))
    (is (= (.getHour result) 11))
    (is (= (.getSecond result) 47))))

(deftest test-parse-to-string-current-offsetdate
  (let [result (util/date->string)]
    (is (= (type result) String))
    (is (util/date-time result))))

(deftest test-parse-to-string-offsetdate
  (let [expected (util/date-time "2020-03-03T11:24:47.473+01:00")
        result (util/date->string expected)]
    (is (= (type result) String))
    (is (= expected (util/date-time result)))))

(def risk-on-url "https://glms-risk-on-svc.example.com/query")
(defn post-response [url body] (future
                                 {:opts   {:body   body,
                                           :method :post,
                                           :url    risk-on-url},
                                  :body   "{\"result\":{\"id\":\"#3cd53114-1a56-427f-99a1-a5512c8e15c1\",\"cocunut\":\"123134\"}}",
                                  :status 200}))

(deftest test-post-call
  (with-redefs [http/post (fn [url req] (post-response url (:body req)))]
    (let [expected (util/http-post risk-on-url {:query-id :get-by-id
                                                :id       "123134"})]
      (is (get-in expected [:body :result]))
      (is (= "123134" (get-in expected [:body :result :cocunut]))))))

(deftest test-escpe
  (is (= (util/escape "\"a\":\"b\"")
         "\\\"a\\\":\\\"b\\\"")))

(deftest test-wrap
  (is (= {:body "{\"a\":\":b\"}"}
         (util/wrap-body {:a :b})))

  (is (= {:form-params {:a :b}}
         (util/wrap-body {:form-params {:a :b}})))

  (is (= {:body    "sas"
          :headers "application/x-www-form-urlencoded"}
         (util/wrap-body {:body    "sas"
                          :headers "application/x-www-form-urlencoded"})))
  (is (= {:body {:a :b}}
         (util/wrap-body {:body {:a :b}}))))

(deftest base64-encode-test
  (is (= "c2RqbGE3Ly9cfiY/IQ=="
         (util/base64encode "sdjla7//\\~&?!"))))

(deftest base64-decode-test
  (is (= "sdjla7//\\~&?!kshkl2njken12kdn21jkd 2km1 m,12 213"
         (util/base64decode "c2RqbGE3Ly9cfiY/IWtzaGtsMm5qa2VuMTJrZG4yMWprZCAya20xIG0sMTIgMjEz"))))

(deftest hmac256-test
  (is (= "V5OClWSQlzec3bOC3WyC1eBGBkWo/QFnSkinbeYUJkY="
         (util/hmac-sha256 "secret123" "hello world"))))

(def user-pool-client-id "48lks9h1rd3kv2f12v0ouvg4ud")
(def user-pool-id "eu-west-1_Btgyjpp8Q")
(def user-pool-client-secret "1s5qeke2jhstfjeqttop9oj7okms6nt9tn9m6h02p6jsvbe1r7ql")

(deftest hmac256-test
  (is (= "qyhnsYcOHY/OCsTqeoE5rMrV5gWYJwTErkvs5l6Yyrk="
         (util/hmac-sha256 user-pool-client-secret
                           (str "test-svc@internal"
                                user-pool-client-id)))))
