(ns lambda.test.fixture.client-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.test.fixture.client :as client]))

(deftest test-mock-app
  (client/mock-http
   [{:get  "http://google.com"
     :body (util/to-json {:a :b})}]
   (is (= {:body {:a :b}}
          (util/http-get "http://google.com" {})))

   (client/verify-traffic
    [{:method :get
      :url    "http://google.com"}])))

(deftest test-mock-multiple-calls
  (client/mock-http
   [{:get  "http://google.com"
     :body (util/to-json {:a :b})}
    {:get  "http://google.com"
     :body (util/to-json {:a :c})}]
   (is (= {:body {:a :b}}
          (util/http-get "http://google.com" {})))

   (is (= {:body {:a :c}}
          (util/http-get "http://google.com" {})))

   (client/verify-traffic
    [{:method :get
      :url    "http://google.com"}
     {:method :get
      :url    "http://google.com"}])))

(deftest test-mock-multiple-calls-non-removable
  (client/mock-http
   {:responses [{:get  "http://google.com"
                 :body (util/to-json {:a :b})}]
    :config {:reuse-responses true}}
   (is (= {:body {:a :b}}
          (util/http-get "http://google.com" {})))

   (is (= {:body {:a :b}}
          (util/http-get "http://google.com" {})))

   (client/verify-traffic
    [{:method :get
      :url    "http://google.com"}
     {:method :get
      :url    "http://google.com"}])))

(deftest find-first-test
  (is (= 1
         (client/find-first ["a" "b" "c"]
                            #(= % "b")))))

(deftest test-mock-post-calls
  (client/mock-http
   [{:post "http://google.com"
     :req  {:request :payload1}
     :body (util/to-json {:a :1})}
    {:post "http://google.com"
     :req  {:request :payload2}
     :body (util/to-json {:a :2})}]

   (is (= {:body {:a :2}}
          (util/http-post "http://google.com" {:request :payload2})))

   (is (= {:body {:a :1}}
          (util/http-post "http://google.com" {:request :payload1})))

   (client/verify-traffic
    [{:body   "{\"request\":\":payload1\"}"
      :method :post
      :url    "http://google.com"}
     {:body   "{\"request\":\":payload2\"}"
      :method :post
      :url    "http://google.com"}])))

(deftest test-mock-no-json
  (let [request-body "Action=Bla"]
    (client/mock-http
     [{:post "http://google.com"
       :req request-body
       :body (util/to-json {:a :b})}]
     (is (= {:body {:a :b}}
            (util/http-post "http://google.com" {:body request-body})))

     (client/verify-traffic
      [{:method :post
        :body request-body
        :url    "http://google.com"}]))))

(deftest test-mock-no-no-body-check
  (let [request-body "random body"]
    (client/mock-http
     [{:post "http://google.com"
       :body (util/to-json {:a :b})}]
     (is (= {:body {:a :b}}
            (util/http-post "http://google.com" {:body request-body})))

     (client/verify-traffic
      [{:method :post
        :body request-body
        :url    "http://google.com"}]))))
