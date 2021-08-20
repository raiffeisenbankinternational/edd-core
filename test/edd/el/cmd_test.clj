(ns edd.el.cmd-test
  (:require [clojure.test :refer :all]
            [edd.el.cmd :as el-cmd]
            [edd.core :as edd]
            [lambda.core :as core]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [lambda.test.fixture.client :refer [verify-traffic-json]]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.api-test :refer [api-request]]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [sdk.aws.sqs :as sqs]))

(deftest test-empty-commands-list
  (let [ctx {:req {}}]
    (is (= (assoc ctx :error {:commands :empty})
           (el-cmd/validate-commands ctx)))))

(deftest test-invalid-command
  (let [ctx {:commands [{}]}]
    (is (= (assoc ctx :error {:spec [{:cmd-id ["missing required key"]}]})
           (el-cmd/validate-commands ctx)))))

(deftest test-invalid-cmd-id-type
  (let [ctx {:commands [{:cmd-id "wrong"}]}]
    (is (= (assoc ctx :error {:spec [{:cmd-id ["should be a keyword"]}]})
           (el-cmd/validate-commands ctx))))
  (let [ctx {:commands [{:cmd-id :test}]}]
    (is (= (assoc ctx :error {:spec
                              [{:unknown-command-handler
                                :test}]})
           (el-cmd/validate-commands ctx)))))

(deftest test-missing-command
  (let [ctx {:commands [{:cmd-id :random-command}]}]
    (is (= (assoc ctx :error {:spec
                              [{:unknown-command-handler
                                :random-command}]})
           (el-cmd/validate-commands ctx))))
  (let [ctx {:commands [{:cmd-id :test}]}]
    (is (= (assoc ctx :error {:spec
                              [{:unknown-command-handler
                                :test}]})
           (el-cmd/validate-commands ctx)))))

(deftest test-custom-schema
  (let [ctx {:spec {:test [:map [:name string?]]}}
        cmd-missing (assoc ctx :commands [{:cmd-id :test}])
        cmd-invalid (assoc ctx :commands [{:cmd-id :test
                                           :name   :wrong}])
        cmd-valid (-> ctx
                      (assoc-in [:command-handlers :test] (fn []))
                      (assoc
                       :commands [{:cmd-id :test
                                   :name   "name"}]))]

    (is (= (assoc cmd-missing :error {:spec [{:name ["missing required key"]}]})
           (el-cmd/validate-commands cmd-missing)))

    (is (= (assoc cmd-invalid :error {:spec [{:name ["should be a string"]}]})
           (el-cmd/validate-commands cmd-invalid)))
    (is (= cmd-valid
           (el-cmd/validate-commands cmd-valid)))))

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
       (verify-traffic-json
        [{:body   {:body            (util/to-json
                                     {:error          {:spec [{:id ["missing required key"]}]}
                                      :invocation-id 0
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
           (verify-traffic-json
            [{:body   {:body            (util/to-json
                                         {:result         {:success    true
                                                           :effects    []
                                                           :events     1
                                                           :meta       [{:ping {:id id}}]
                                                           :identities 0
                                                           :sequences  0}
                                          :invocation-id 0
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
