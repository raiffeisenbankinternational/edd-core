(ns edd.el.cmd-test
  (:require [clojure.test :refer [deftest is testing]]
            [edd.el.cmd :as el-cmd]
            [edd.core :as edd]
            [lambda.core :as core]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [lambda.test.fixture.client :refer [verify-traffic-edn] :as client]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.api-test :refer [api-request]]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [sdk.aws.sqs :as sqs]
            [edd.el.ctx :as el-ctx]
            [lambda.request :as request]
            [aws.aws :as aws])
  (:import (clojure.lang ExceptionInfo)))

(def ctx (-> {}
             (edd/reg-cmd :ssa (fn [_ _]))))

(deftest test-empty-commands-list
  (try
    (el-cmd/validate-commands ctx [])
    (catch ExceptionInfo ex
      (is (= {:error "No commands present in request"}
             (ex-data ex))))))

(defn register []
  (edd/reg-cmd mock/ctx
               :ping
               (fn [_ _] {:event-id :ping})))

(deftest api-handler-test
  (let [request-id (uuid/gen)
        interaction-id (uuid/gen)
        cmd {:request-id     request-id,
             :interaction-id interaction-id,
             :commands       [{:cmd-id :ping}]}]
    (mock-core
     :env {"Region" "eu-west-1"}
     :invocations [(api-request cmd)]
     (core/start
      (register)
      edd/handler
      :filters [fl/from-api]
      :post-filter fl/to-api)

     (is (= [{:body   {:body            {:invocation-id  0
                                         :request-id     request-id
                                         :interaction-id interaction-id
                                         :error          {:id ["missing required key"]}}
                       :headers         {:Access-Control-Allow-Headers  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                                         :Access-Control-Allow-Methods  "OPTIONS,POST,PUT,GET"
                                         :Access-Control-Allow-Origin   "*"
                                         :Access-Control-Expose-Headers "*"
                                         :Content-Type                  "application/json"}
                       :isBase64Encoded false
                       :statusCode      500}
              :method :post
              :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
             {:url
              "https://sqs.eu-west-1.amazonaws.com/local/local-glms-router-svc-response",
              :method :post,
              :body
              (str "Action=SendMessage&MessageBody=%7B%22Records%22%3A%5B%7B%22key%22%3A%22response%2F"
                   request-id
                   "%2F0%2Flocal-test.json%22%7D%5D%7D")}
             {:method  :get
              :url     "http://mock/2018-06-01/runtime/invocation/next"}]
            (mapv
             #(select-keys % [:method :body :url])
             (client/traffic-edn)))))))

(deftest api-handler-response-test

  (let [request-id (uuid/gen)
        interaction-id (uuid/gen)
        id (uuid/gen)
        cmd {:request-id     request-id,
             :interaction-id interaction-id,
             :user           {:selected-role :group-1}
             :commands       [{:cmd-id :ping
                               :id     id}]}]
    (mock/with-mock-dal
      (with-redefs [sqs/sqs-publish (fn [{:keys [message]}]
                                      (is (= {:Records [{:key (str "response/"
                                                                   request-id
                                                                   "/0/local-test.json")}]}
                                             (util/to-edn message))))]
        (mock-core
         :env {"Region" "eu-west-1"}
         :invocations [(api-request cmd)]
         (core/start
          (register)
          edd/handler
          :filters [fl/from-api]
          :post-filter fl/to-api)
         (do
           (mock/verify-state :event-store [{:event-id  :ping
                                             :event-seq 1
                                             :id        id
                                             :meta      {:realm :test
                                                         :user  {:email "john.smith@example.com"
                                                                 :id    "john.smith@example.com"
                                                                 :role  :group-1
                                                                 :roles [:group-1
                                                                         :group-3
                                                                         :group-2]}}}])
           (verify-traffic-edn
            [{:body   {:body            (util/to-json
                                         {:result         {:success    true
                                                           :effects    []
                                                           :events     1
                                                           :meta       [{:ping {:id id}}]
                                                           :identities 0
                                                           :sequences  0}
                                          :invocation-id  0
                                          :request-id     request-id
                                          :interaction-id interaction-id})
                       :headers         {:Access-Control-Allow-Headers  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                                         :Access-Control-Allow-Methods  "OPTIONS,POST,PUT,GET"
                                         :Access-Control-Allow-Origin   "*"
                                         :Access-Control-Expose-Headers "*"
                                         :Content-Type                  "application/json"}
                       :isBase64Encoded false
                       :statusCode      200}
              :method :post
              :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
             {:method  :get
              :timeout 90000000
              :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))))

(deftest test-cache-partitioning
  (let [ctx {:service-name "local-test"
             :breadcrumbs  "0"
             :request-id   "1"}
        resp-1 {:effects [{:a :b}]}

        resp-2 {:effects [{:a :b} {:a :b} {:a :b}]}]
    (is (= (assoc resp-1
                  :cache-result
                  {:key "response/1/0/local-test.json"})
           (el-cmd/resp->cache-partitioned ctx resp-1)))
    (is (= (assoc resp-2
                  :cache-result
                  [{:key "response/1/0/local-test-part.0.json"}
                   {:key "response/1/0/local-test-part.1.json"}])
           (el-cmd/resp->cache-partitioned (el-ctx/set-effect-partition-size ctx 2)
                                           resp-2)))))

(deftest enqueue-response
  (binding [request/*request* (atom {:cache-keys [{:key "response/0/0/local-test-0.json"}
                                                  {:key "response/1/0/local-test-part.0.json"}
                                                  {:key "response/1/0/local-test-part.1.json"}
                                                  {:key "response/2/0/local-test-2.json"}]})]
    (let [messages (atom [])]
      (with-redefs [sqs/sqs-publish (fn [{:keys [message]}]
                                      (swap! messages #(conj % message)))]
        (aws/enqueue-response ctx {})
        (is (= [{:Records [{:key "response/0/0/local-test-0.json"}]}
                {:Records [{:key "response/1/0/local-test-part.0.json"}]}
                {:Records [{:key "response/1/0/local-test-part.1.json"}]}
                {:Records [{:key "response/2/0/local-test-2.json"}]}]
               (map
                #(util/to-edn %)
                @messages))))))
  (binding [request/*request* (atom {:cache-keys [{:key "response/0/0/local-test-0.json"}
                                                  [{:key "response/1/0/local-test-part.0.json"}
                                                   {:key "response/1/0/local-test-part.1.json"}]
                                                  {:key "response/2/0/local-test-2.json"}]})]
    (let [messages (atom [])]
      (with-redefs [sqs/sqs-publish (fn [{:keys [message]}]
                                      (swap! messages #(conj % message)))]
        (aws/enqueue-response ctx {})
        (is (= [{:Records [{:key "response/0/0/local-test-0.json"}]}
                {:Records [{:key "response/1/0/local-test-part.0.json"}]}
                {:Records [{:key "response/1/0/local-test-part.1.json"}]}
                {:Records [{:key "response/2/0/local-test-2.json"}]}]
               (map
                #(util/to-edn %)
                @messages)))))))

(deftest test-retry-logic
  (testing "regular exception"
    (let [capture! (atom 0)]
      (try
        (el-cmd/retry (fn []
                        (swap! capture! inc)
                        (/ 0 0))
                      3)
        (is false)
        (catch Exception e
          (is (= 1 @capture!))))))

  (testing "special exception"
    (let [capture! (atom 0)]
      (try
        (el-cmd/retry
         (fn []
           (swap! capture! inc)
           (throw (ex-info "boom" {:error {:key :concurrent-modification}})))
         3)
        (is false)
        (catch Exception e
          (is (= 3 @capture!))
          (is (= "boom"
                 (ex-message e))))))))
