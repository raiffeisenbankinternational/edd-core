(ns edd.schema-test
  (:require [edd.schema :as s]
            [malli.core :as m]
            [clojure.test :refer :all]))

(def test-date-schema
  (m/schema
   [:map
    [:id keyword?]
    [:current-date s/date?]]))

(def valid-command
  {:cmd-id :valid
   :id #uuid "2812c77d-588c-4808-b10f-ed4836012882"})

(def valid-command-extra-fields
  {:cmd-id :valid
   :id #uuid "2812c77d-588c-4808-b10f-ed4836012882"
   :atrrs {:value "a"}})

(def invalid-command
  {:cmd-id ":valid"
   :id #uuid "2812c77d-588c-4808-b10f-ed4836012882"})

(deftest decode-command-schema
  (let [decoded (s/decode s/command-schema valid-command)]
    (is (m/validate s/command-schema decoded))))

(deftest decode-command-schema-with-extra-fields
  (let [decoded (s/decode s/command-schema valid-command-extra-fields)]
    (is (m/validate s/command-schema decoded))))

(deftest decode-invalid-command-schema
  (let [decoded (s/decode s/command-schema invalid-command)]
    (is (not (m/validate s/command-schema decoded)))))

(deftest valid-date-schema
  (testing "a valid schema with date like YYYY-MM-DD")
  (let [decoded (s/decode test-date-schema {:id :date
                                            :current-date "2020-01-01"})]
    (is (m/validate test-date-schema decoded))))

(deftest invalid-date-schema
  (testing "an invalid schema with date like DD-MM-YYYY")
  (let [decoded (s/decode test-date-schema {:id :date
                                            :current-date "01-01-2020"})]
    (is (not (m/validate test-date-schema decoded)))))
