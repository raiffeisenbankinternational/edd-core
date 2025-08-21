(ns edd.json.core-test
  "
  JSON-related tests borrowed from the lambda.util-test namespace.
  "
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest testing is]]
   [edd.json.core :as json])
  (:import
   (java.io ByteArrayOutputStream)
   (mikera.vectorz Vector)))

(deftest test-parser-deserialization
  (let [result (json/to-edn
                "{\"json\": true, \"converted\": { \"edn\": true, \"bla\" : \":ble\",\"blo\":\"::bli\", \"li\":[\"a\", \":a\",\"::a\"]}}")]
    (is (= result {:json true,
                   :converted
                   {:edn true,
                    :bla :ble,
                    :blo ":bli",
                    :li  ["a" :a ":a"]}}))))

(deftest test-json-output-write-and-read
  (let [data-old
        {:foo {:test 42}}

        out
        (new ByteArrayOutputStream)

        _
        (json/to-json-out out data-old)

        data-new
        (-> out
            .toByteArray
            io/input-stream
            json/to-edn)]

    (is (= data-old data-new))))

(deftest test-parser-array-keys-serialization
  (let [result (json/to-edn
                "{\"request-id\":\"#780b4fdd-581f-4261-8d6f-09b92ef97c09\",\"interaction-id\":\"#6e97b6a3-1078-4e1f-a395-522d6a50e44a\",\"query\":{\"query-id\":\":list-risks-on\",\"search\":[\":fields\",[\"attrs.cocunut\"],\":value\",\"109\"]},\"user\":{\"selected-role\":\":lime-sem-experts\"}}")]
    (is (= {:interaction-id #uuid "6e97b6a3-1078-4e1f-a395-522d6a50e44a"
            :query          {:query-id :list-risks-on
                             :search   [:fields ["attrs.cocunut"]
                                        :value "109"]}
            :request-id     #uuid "780b4fdd-581f-4261-8d6f-09b92ef97c09"
            :user           {:selected-role :lime-sem-experts}}
           result))))

(deftest test-parser-serialization
  (let [result (json/to-json {:json true,
                              :converted
                              {:edn true,
                               :bla :ble,
                               :blo ":bli",
                               :li  ["a" :a ":a"]}})]
    (is (= result "{\"json\":true,\"converted\":{\"edn\":true,\"bla\":\":ble\",\"blo\":\"::bli\",\"li\":[\"a\",\":a\",\"::a\"]}}"))))

(deftest test-uuid-deserialization
  (let [result (json/to-edn
                "{\"json\": \"#c2baf5fb-1268-4a25-bd12-ece185e86104\"}")]
    (is (= result {:json #uuid "c2baf5fb-1268-4a25-bd12-ece185e86104"}))))

(deftest test-uuid-as-key-serialization
  (testing "Rountrip of uuid keys should work"
    (is
     (=
      {#uuid "4c41737a-498e-4ed9-9408-59a3cdddbae3"
       #uuid "4c41737a-498e-4ed9-9408-59a3cdddbae3"
       :nested {:edn true,
                :bla :ble,
                :blo ":bli",
                :li  ["a" :a ":a"]}}
      (->
       {#uuid "4c41737a-498e-4ed9-9408-59a3cdddbae3"
        #uuid "4c41737a-498e-4ed9-9408-59a3cdddbae3"
        :nested {:edn true,
                 :bla :ble,
                 :blo ":bli",
                 :li  ["a" :a ":a"]}}
       (json/to-json)
       (json/to-edn))))))

(deftest test-double-serialization
  (is (= "{\"c\":\":d\",\"e\":\"{\\\"a\\\":\\\":b\\\"}\"}"
         (json/to-json
          {:c :d
           :e (json/to-json
               {:a :b})}))))

(deftest test-parser-serialization-uuid
  (let [result (json/to-json {:json #uuid "c2baf5fb-1268-4a25-bd12-ece185e86104"})]
    (is (= result "{\"json\":\"#c2baf5fb-1268-4a25-bd12-ece185e86104\"}"))))

(deftest test-vectorz-serialization
  (let [vector (Vector/of (double-array [1.0 2.0 3.0]))
        result (json/to-json {:vectorz vector})]
    (is (= "{\"vectorz\":[1.0,2.0,3.0]}"
           result))))
