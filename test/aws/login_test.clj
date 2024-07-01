(ns aws.login-test
  (:require
   [aws.login :as login]
   [clojure.test :refer [deftest is]]
   [lambda.util :as util]))

(deftest test-get-id-token-ok

  (let [capture!
        (atom nil)

        result
        (with-redefs [util/http-get
                      (fn [url params & args]
                        (reset! capture! {:url url
                                          :params params
                                          :args args})
                        {:status 200
                         :body (util/to-json {:id-token "secret-token"})})]

          (login/get-id-token "john"
                              "Smith123"
                              "lime-dev19.internal.rbigroup.cloud"))]

    (is (= "secret-token" result))

    (is (= {:url "https://glms-login-svc.lime-dev19.internal.rbigroup.cloud"
            :params
            {:query-params {"json" "true" "user-password-auth" "true"}
             :body "{\"username\":\"john\",\"password\":\"Smith123\"}"}
            :args [:raw true]}
           @capture!))))

(deftest test-get-id-token-error

  (try
    (with-redefs [util/http-get
                  (constantly {:status 301 :body ""})]

      (login/get-id-token "john"
                          "Smith123"
                          "lime-dev19.internal.rbigroup.cloud"))
    (is false)
    (catch Throwable e
      (is (= "GLMS login service error, status: 301, url: https://glms-login-svc.lime-dev19.internal.rbigroup.cloud, user: john"
             (ex-message e)))
      (is (= {:username "john"
              :url "https://glms-login-svc.lime-dev19.internal.rbigroup.cloud"
              :status 301
              :body ""}
             (ex-data e))))))
