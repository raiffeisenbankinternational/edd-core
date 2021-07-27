(ns edd.el.query-test
  (:require [clojure.test :refer :all]
            [edd.core :as edd]
            [edd.el.query :as query]
            [lambda.core :as core]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [lambda.test.fixture.client :refer [verify-traffic-json]]
            [lambda.test.fixture.core :refer [realm-mock mock-core]]
            [lambda.api-test :refer [api-request]]
            [lambda.uuid :as uuid]
            [edd.test.fixture.dal :as mock]
            [sdk.aws.sqs :as sqs]
            [clojure.tools.logging :as log]
            [lambda.jwt :as jwt]))

(deftest test-if-meta-is-resolved-to-query

  (let [request-id (uuid/gen)
        interaction-id (uuid/gen)
        meta {:realm :realm11
              :some-other-stuff :yes}
        cmd {:request-id request-id,
             :interaction-id interaction-id,
             :meta meta
             :query [{:query-id :get-by-id}]}]
    (mock/with-mock-dal
      (with-redefs [realm-mock fl/get-realm
                    jwt/parse-token (fn [ctx _]
                                      (assoc ctx
                                             :user {:id ""
                                                    :email ""
                                                    :roles [:non-interactive :realm-test]}))
                    sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                      (is (= {:Records [{:key (str "response/"
                                                                   request-id
                                                                   "/0/local-test.json")}]}
                                             (util/to-edn message))))
                    query/handle-query (fn [ctx body]
                                         (is (= cmd
                                                body))
                                         (is (= meta
                                                (:meta ctx))))]
        (mock-core
         :invocations [(api-request cmd)]
         (core/start
          mock/ctx
          edd/handler
          :filters [fl/from-api]
          :post-filter fl/to-api)
         (do
           (log/info "Nothing nere to check")))))))

(deftest test-query-when-missing-selected-role

  (let [request-id (uuid/gen)
        interaction-id (uuid/gen)
        realm :realm11
        user {:id ""
              :email ""
              :roles [:anonymous :account-manager :realm-realm11]}
        meta {:realm realm
              :user user}
        cmd {:request-id request-id,
             :interaction-id interaction-id,
             :meta meta
             :query [{:query-id :get-by-id}]}]
    (mock/with-mock-dal
      (with-redefs [realm-mock fl/get-realm
                    jwt/parse-token (fn [ctx _]
                                      (assoc ctx
                                             :user user))
                    sqs/sqs-publish (fn [{:keys [message] :as ctx}]
                                      (is (= {:Records [{:key (str "response/"
                                                                   request-id
                                                                   "/0/local-test.json")}]}
                                             (util/to-edn message))))
                    query/handle-query (fn [ctx body]
                                         (is (= cmd
                                                body))
                                         (is (= {:realm realm
                                                 :user {:email ""
                                                        :id ""
                                                        :role :account-manager}}
                                                (:meta ctx))))]
        (mock-core
         :invocations [(api-request cmd)]
         (core/start
          mock/ctx
          edd/handler
          :filters [fl/from-api]
          :post-filter fl/to-api)
         (do
           (log/info "Nothing nere to check")))))))