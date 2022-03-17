(ns edd.el.cmd-test
  (:require [clojure.test :refer :all]
            [edd.el.cmd :as el-cmd]
            [edd.core :as edd]
            [lambda.core :as core]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.client :refer [verify-traffic-edn]]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.api-test :refer [api-request]]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [sdk.aws.sqs :as sqs]
            [edd.el.cmd :as cmd]
            [edd.el.ctx :as el-ctx]
            [lambda.request :as request]
            [aws.aws :as aws])
  (:import (clojure.lang ExceptionInfo)))

(def ctx (-> {}
             (edd/reg-cmd :ssa (fn [ctx cmd]))))

(deftest test-empty-commands-list
  (try
    (el-cmd/validate-commands ctx [])
    (catch ExceptionInfo ex
      (is (= {:error "No commands present in request"}
             (ex-data ex))))))

(deftest test-invalid-command
  (try
    (el-cmd/validate-commands ctx [{}])
    (catch ExceptionInfo ex
      (is (= {:error [{:cmd-id ["missing required key"]
                       :id     ["missing required key"]}]}
             (ex-data ex))))))

(deftest test-invalid-cmd-id-type
  (try
    (el-cmd/validate-commands ctx [{:id     (uuid/gen)
                                    :cmd-id "wrong"}])
    (catch ExceptionInfo ex
      (is (= {:error [{:cmd-id ["should be a keyword"]}]}
             (ex-data ex)))))

  (try
    (el-cmd/validate-commands ctx [{:cmd-id :test
                                    :id     (uuid/gen)}])
    (catch ExceptionInfo ex
      (is (= {:error ["Missing handler: :test"]}
             (ex-data ex))))))

(deftest test-custom-schema
  (let [ctx (edd/reg-cmd ctx
                         :test (fn [_ _])
                         :consumes [:map [:name :string]])
        cmd-missing {:cmd-id :test-unknown
                     :id     (uuid/gen)}
        cmd-invalid {:cmd-id :test
                     :name   :wrong}
        cmd-valid {:cmd-id :test
                   :name   "name"
                   :id     (uuid/gen)}]

    (try
      (el-cmd/validate-commands ctx [cmd-missing cmd-invalid cmd-valid])
      (catch ExceptionInfo ex
        (is (= {:error ["Missing handler: :test-unknown"
                        {:id ["missing required key"]}
                        :valid]}
               (ex-data ex)))))))

(defn register
  []
  (-> mock/ctx
      (edd/reg-cmd :ping
                   (fn [ctx cmd]
                     {:event-id :ping}))))

(deftest api-handler-test
  (let [request-id (uuid/gen)
        interaction-id (uuid/gen)
        cmd {:request-id     request-id,
             :interaction-id interaction-id,
             :commands       [{:cmd-id :ping}]}]
    (mock-core
     :invocations [(api-request cmd)]
     (core/start
      (register)
      edd/handler
      :filters [fl/from-api]
      :post-filter fl/to-api)
     (do
       (verify-traffic-edn
        [{:body   {:body            (util/to-json
                                     {:invocation-id  0
                                      :request-id     request-id
                                      :interaction-id interaction-id
                                      :error          [{:id ["missing required key"]}]})

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
          :url     "http://mock/2018-06-01/runtime/invocation/next"}])))))

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
      (with-redefs [sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                      (is (= {:Records [{:key (str "response/"
                                                                   request-id
                                                                   "/0/local-test.json")}]}
                                             (util/to-edn message))))]
        (mock-core
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
                                                                 :role  :group-1}}
                                             :role      :group-1
                                             :user      "john.smith@example.com"}])
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
             :request-id   "1"}]
    (is (= {:key "response/1/0/local-test.json"}
           (cmd/resp->cache-partitioned ctx {:effects [{:a :b}]})))
    (is (= [{:key "response/1/0/local-test-part.0.json"}
            {:key "response/1/0/local-test-part.1.json"}]
           (cmd/resp->cache-partitioned (el-ctx/set-effect-partition-size ctx 2)
                                        {:effects [{:a :b} {:a :b} {:a :b}]})))))

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