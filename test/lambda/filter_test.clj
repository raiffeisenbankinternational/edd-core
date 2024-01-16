(ns lambda.filter-test
  (:require [clojure.test :refer :all]
            [lambda.codec :as codec]
            [lambda.filters :as lambda-filter]
            [lambda.jwt-test :as jwt-test]
            [lambda.util :as util]))

(def claims
  {:cognito:groups ["lime-risk-managers"
                    "lime-account-managers"
                    "lime-limit-managers"
                    "roles-admin"
                    "realm-test"
                    "units-rbhu"
                    "units-ho"
                    "roles-verifiers"]
   :email          "john.smith@rbinternational.com"
   :exp            1647513070
   :sub            "000d0c9d-d5f1-4cbd-adb2-fa9480be2346"
   :token_use      "id"})

(deftest test-extract-attrs
  (is (= {:department      "DEV1"
          :department-code "dep-code"}
         (lambda-filter/extract-attrs {:id                       "za"
                                       :groups                   [:g]
                                       :x-department             "DEV1"
                                       :custom:x-department_code "dep-code"}))))

(deftest extract-user-test
  (is (= {:email "john.smith@rbinternational.com"
          :id "john.smith@rbinternational.com"
          :realm :test
          :units [:ho :rbhu]
          :roles [:verifiers
                  :admin
                  :lime-limit-managers
                  :lime-account-managers
                  :lime-risk-managers]}
         (lambda-filter/extract-user {} claims))))

(deftest parse-authorizer-user-test
  (is (= {:email "john.smith@rbinternational.com"
          :id "john.smith@rbinternational.com"
          :realm :test
          :role :verifiers
          :roles '(:verifiers
                   :admin
                   :lime-limit-managers
                   :lime-account-managers
                   :lime-risk-managers)
          :units '(:ho
                   :rbhu)}
         (lambda-filter/parse-authorizer-user {} claims)))
  (is (thrown? Exception
               (lambda-filter/parse-authorizer-user {:body {:user {:selected-role :non-existing-role}}} claims))))

(deftest extract-user-test-2
  (let [claims {:id             ""
                :email          ""
                :cognito:groups ["non-interactive" "realm-test"]}]
    (is (= {:email ""
            :id    ""
            :realm :test
            :roles [:non-interactive]}
           (lambda-filter/extract-user {} claims)))
    (is (= {:email ""
            :id    ""
            :realm :test
            :role :non-existing-role
            :roles [:non-interactive]}
           (lambda-filter/parse-authorizer-user {:body {:user {:selected-role :non-existing-role}}} claims)))))

(deftest extract-user-with-department
  (let [claims {:department      "dep"
                :department-code "dcode"
                :email           "john@example.com"}]
    (is (= {:department      "dep"
            :department-code "dcode"}
           (lambda-filter/extract-attrs claims)))

    (is (= {:department-code "dcode"}
           (lambda-filter/extract-attrs {:department_code "dcode"})))))

(deftest test-parse-query
  (let [query-string "address[street]=Main&address[city]=New York"]
    (is (= {:address {:street "Main"
                      :city "New York"}}
           (lambda-filter/parse-query-string query-string))))
  (let [query-string "address[street]=:main&address[city]=New York&address[zip][code]=4210"]
    (is (= {:address {:street :main
                      :city "New York"
                      :zip {:code "4210"}}}
           (lambda-filter/parse-query-string query-string)))))

(defn truncate-response [res]
  (-> res
      (update-in [:resp :headers]
                 select-keys
                 ["Content-Type" "Content-Encoding"])
      (dissoc :req)))

(deftest test-to-api-no-gzip
  (let [res
        (lambda-filter/to-api
         {:resp {:message ["hello" :world]}})]
    (is (= {:resp
            {:statusCode 200
             :headers
             {"Content-Type" "application/json"}
             :isBase64Encoded false
             :body "{\"message\":[\"hello\",\":world\"]}"}}
           (truncate-response res)))))

(deftest test-to-api-gzip
  (let [res
        (lambda-filter/to-api
         {:req {:headers {:Accept-Encoding "Foo; Gzip; Bar"}}
          :resp {:message ["hello" :world]}})

        expected-body
        "H4sIAAAAAAAA/6tWyk0tLk5MT1WyilbKSM3JyVfSUbIqzy/KSVGKrQUA7MSEOh4AAAA="

        decoded
        (-> expected-body
            codec/string->bytes
            codec/base64->bytes
            codec/gzip->bytes
            codec/bytes->string
            util/to-edn)]

    (is (= {:resp
            {:statusCode 200
             :headers
             {"Content-Type" "application/json"
              "Content-Encoding" "gzip"}
             :isBase64Encoded true
             :body expected-body}}
           (truncate-response res)))

    (is (= {:message ["hello" :world]} decoded))))
