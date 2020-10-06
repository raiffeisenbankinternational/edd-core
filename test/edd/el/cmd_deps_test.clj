(ns edd.el.cmd-deps-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [edd.el.cmd :as cmd]
            [edd.dal :as dal]
            [edd.el.cmd :as cmd]
            [edd.core :as edd]
            [lambda.test.fixture.client :as client]
            [edd.test.fixture.dal :as mock]))

(def cmd-id #uuid "11111eeb-e677-4d73-a10a-1d08b45fe4dd")

(def request-id #uuid "22222eeb-e677-4d73-a10a-1d08b45fe4dd")
(def interaction-id #uuid "33333eeb-e677-4d73-a10a-1d08b45fe4dd")

(deftest test-prepare-context-for-command-with-remote-query
  "Test if context if properly prepared for remote queries"
  (mock/with-mock-dal
    {:dps [{:service :remote-svc
            :request-id     request-id
            :interaction-id interaction-id
            :query   {:param "Some Value"}
            :resp    {:remote :response}}]}
    (let [ctx (cmd/resolve-dependencies-to-context
               {:dps            {:test-cmd
                                 {:test-value {:query   (fn [cmd]
                                                          {:param (:value cmd)})
                                               :service :remote-svc}}}
                :request-id     request-id
                :interaction-id interaction-id}
               {:commands [{:cmd-id :test-cmd
                            :id     cmd-id
                            :value  "Some Value"}]})]
      (is (= {:request-id     request-id
              :interaction-id interaction-id
              :dps-resolved   [{:test-value
                                {:remote :response}}]}
             (dissoc ctx :dps)))

      (client/verify-traffic [{:body    (util/to-json
                                         {:query          {:param "Some Value"}
                                          :request-id     request-id
                                          :interaction-id interaction-id})
                               :headers {"X-Authorization" "#mock-id-token"
                                         "Content-Type"    "application/json"}
                               :method  :post
                               :timeout 10000
                               :url     "https://remote-svc./query"}]))))

(deftest test-remote-dependency
  (with-redefs [aws/get-token (fn [ctx] "#id-token")
                dal/log-dps (fn [ctx] ctx)
                util/get-env (fn [v]
                               (get {"PrivateHostedZoneName" "mock.com"} v))]
    (client/mock-http
     [{:post "https://some-remote-service.mock.com/query"
       :body (util/to-json {:result {:a :b}})}]
     (is (= {:a :b}
            (cmd/resolve-remote-dependency
             {:request-id     request-id
              :interaction-id interaction-id}
             {}
             {:query   (fn [cmd] {:a :b})
              :service :some-remote-service})))
     (client/verify-traffic [{:body    (util/to-json
                                        {:query          {:a :b}
                                         :request-id     request-id
                                         :interaction-id interaction-id})
                              :headers {"X-Authorization" "#id-token"
                                        "Content-Type"    "application/json"}
                              :method  :post
                              :timeout 10000
                              :url     "https://some-remote-service.mock.com/query"}]))))



(def cmd-id-1 #uuid "11111eeb-e677-4d73-a10a-1d08b45fe4dd")
(def cmd-id-deps #uuid "33333eeb-e677-4d73-a10a-1d08b45fe4dd")

(def cmd-1-deps
  {cmd-id-1 {:id cmd-id-deps}})

(def cmd-1
  {:cmd-id :cmd-1
   :value  :1
   :id     cmd-id-1})

(def cmd-id-2 #uuid "22222eeb-e677-4d73-a10a-1d08b45fe4dd")
(def cmd-2
  {:cmd-id :cmd-2
   :id     cmd-id-2})

(def ctx (-> {}
             (edd/reg-query :query-1 (fn [ctx query]
                                       (get
                                        cmd-1-deps
                                        (:id query))))
             (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                   {:event-id :event-1
                                    :value    (:value cmd)})
                          :dps {:c1 (fn [cmd] {:query-id :query-1
                                               :id       (:id cmd)})}
                          :id-fn (fn [ctx cmd]
                                   (get-in ctx [:c1 :id])))))

(deftest test-id-fn
  (mock/with-mock-dal
    (cmd/get-commands-response ctx
                               {:commands [cmd-1
                                           {:cmd-id :cmd-1
                                            :value  :2
                                            :id     cmd-id-2}]})
    (mock/verify-state :event-store [{:event-id  :event-1
                                      :event-seq 1
                                      :value     :2
                                      :id        cmd-id-2}
                                     {:event-id  :event-1
                                      :event-seq 1
                                      :value     :1
                                      :id        cmd-id-deps}])))