(ns lambda.filter-test
  (:require [clojure.test :refer :all]
            [lambda.filters :as lambda-filter]
            [lambda.jwt-test :as jwt-test]))

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
