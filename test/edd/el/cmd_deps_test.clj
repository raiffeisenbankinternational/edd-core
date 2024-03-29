(ns edd.el.cmd-deps-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [edd.el.cmd :as cmd]
            [edd.dal :as dal]
            [edd.el.cmd :as cmd]
            [edd.common :as common]
            [edd.core :as edd]
            [lambda.test.fixture.client :as client]
            [edd.memory.event-store :as event-store]
            [edd.test.fixture.dal :as mock]
            [edd.ctx :as edd-ctx]
            [lambda.uuid :as uuid]
            [aws.aws :as aws])
  (:import (java.net URLEncoder)
           (clojure.lang ExceptionInfo)))

(def cmd-id #uuid "11111eeb-e677-4d73-a10a-1d08b45fe4dd")

(def request-id #uuid "22222eeb-e677-4d73-a10a-1d08b45fe4dd")
(def interaction-id #uuid "33333eeb-e677-4d73-a10a-1d08b45fe4dd")

(deftest test-prepare-context-for-command-with-remote-query
  "Test if context if properly prepared for remote queries"
  (let [meta {:realm :realm3}]
    (mock/with-mock-dal
      {:dps [{:service        :remote-svc
              :request-id     request-id
              :interaction-id interaction-id
              :meta           meta
              :query          {:param "Some Value"}
              :resp           {:remote :response}}]}
      (let [cmd {:cmd-id :test-cmd
                 :id     cmd-id
                 :value  "Some Value"}
            ctx (-> {}
                    (edd-ctx/put-cmd :cmd-id :test-cmd
                                     :options {:deps
                                               {:test-value
                                                {:query   (fn [cmd]
                                                            {:param (:value cmd)})
                                                 :service :remote-svc}}})
                    (assoc :meta meta
                           :request-id request-id
                           :interaction-id interaction-id)
                    (event-store/register))
            deps (cmd/fetch-dependencies-for-command
                  ctx
                  cmd)]
        (is (= {:test-value
                {:remote :response}}
               deps))

        (client/verify-traffic [{:body            (util/to-json
                                                   {:query          {:param "Some Value"}
                                                    :meta           meta
                                                    :request-id     request-id
                                                    :interaction-id interaction-id})
                                 :headers         {"X-Authorization" "#mock-id-token"
                                                   "Content-Type"    "application/json"}
                                 :method          :post
                                 :idle-timeout    10000
                                 :connect-timeout 300
                                 :url             "https://remote-svc./query"}])))))

(deftest test-remote-dependency
  (let [meta {:realm :realm4}
        ctx {:request-id     request-id
             :interaction-id interaction-id
             :meta           meta}]
    (with-redefs [aws/get-token (fn [ctx] "#id-token")
                  dal/log-dps (fn [ctx] ctx)
                  util/get-env (fn [v]
                                 (get {"PrivateHostedZoneName" "mock.com"} v))]
      (client/mock-http
       [{:post "https://some-remote-service.mock.com/query"
         :body (util/to-json {:result {:a :b}})}]
       (is (= {:a :b}
              (cmd/resolve-remote-dependency
               ctx
               {}
               {:query   (fn [cmd] {:a :b})
                :service :some-remote-service}
               {})))
       (client/verify-traffic [{:body            (util/to-json
                                                  {:query          {:a :b}
                                                   :meta           meta
                                                   :request-id     request-id
                                                   :interaction-id interaction-id})
                                :headers         {"X-Authorization" "#id-token"
                                                  "Content-Type"    "application/json"}
                                :method          :post
                                :idle-timeout    10000
                                :connect-timeout 300
                                :url             "https://some-remote-service.mock.com/query"}])))))

(deftest test-when-dependency-in-context-should-override

  (testing
   "If depdency is in context (i.e. for tests) resolution should not override it"
    (let [cmd-id-1 #uuid "11111eeb-e677-4d73-a10a-1d08b45fe4dd"
          meta {:realm :realm4}
          ctx (assoc mock/ctx
                     :request-id request-id
                     :interaction-id interaction-id
                     :meta meta
                     :facility {:some :values})
          ctx (edd/reg-cmd ctx
                           :cmd-1 (fn [ctx cmd]
                                    (is (= {:some :values}
                                           (:facility ctx)))
                                    {:event-id :event-1
                                     :value    (:value cmd)})
                           :id-fn (fn [ctx cmd]
                                    (is (= {:some :values}
                                           (:facility ctx)))
                                    cmd-id-1)
                           :deps {:facility (fn [_] {:query-id :query-1})})
          ctx (edd/reg-fx ctx (fn [ctx [event]]
                                (is (= {:some :values}
                                       (:facility ctx)))
                                []))
          ctx (edd/reg-query ctx
                             :query-1 (fn [_ _]
                                        {:some :error}))]
      (mock/with-mock-dal
        (cmd/handle-commands ctx
                             {:commands [{:cmd-id :cmd-1
                                          :id     cmd-id-1}]})))))

(deftest test-remote-dependency-missing
  (testing "What ends up in context when remote dependency is not resolved"
    (let [cmd-id-1 #uuid "11111eeb-e677-4d73-a10a-1d08b45fe4dd"
          meta {:realm :realm4}
          ctx (assoc mock/ctx
                     :request-id request-id
                     :interaction-id interaction-id
                     :meta meta)
          ctx (edd/reg-cmd ctx
                           :cmd-1 (fn [ctx cmd]
                                    (is (= nil
                                           (:facility ctx)))
                                    {:event-id :event-1
                                     :value    (:value cmd)})
                           :id-fn (fn [ctx cmd]
                                    (is (= nil
                                           (:facility ctx)))
                                    cmd-id-1)
                           :deps {:facility {:service :some-service
                                             :query   (fn [_] {:query-id :query-1})}})
          ctx (edd/reg-fx ctx (fn [ctx [event]]
                                (is (= nil
                                       (:facility ctx)))
                                []))
          ctx (edd/reg-query ctx
                             :query-1 (fn [_ _]
                                        {:some :error}))]
      (mock/with-mock-dal
        (cmd/handle-commands ctx
                             {:commands [{:cmd-id :cmd-1
                                          :id     cmd-id-1}]})))))
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

(def ctx (-> mock/ctx
             (edd/reg-query :query-1 (fn [ctx query]
                                       (get
                                        cmd-1-deps
                                        (:id query))))
             (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                   {:event-id :event-1
                                    :value    (:value cmd)})
                          :deps {:c1 (fn [cmd] {:query-id :query-1
                                                :id       (:id cmd)})
                                 :c2 (fn [_] nil)}
                          :id-fn (fn [ctx cmd]
                                   (get-in ctx [:c1 :id])))))

(deftest test-id-fn-integration
  (testing
   "Context defines id-fn for :cmd-1 so we expect that"
    (mock/with-mock-dal
      (cmd/handle-commands ctx
                           {:commands [cmd-1
                                       {:cmd-id :cmd-1
                                        :value  :2
                                        :id     cmd-id-2}]})
      (mock/verify-state :event-store [{:event-id  :event-1
                                        :event-seq 1
                                        :value     :2
                                        :meta      {}
                                        :id        cmd-id-2}
                                       {:event-id  :event-1
                                        :event-seq 1
                                        :value     :1
                                        :meta      {}
                                        :id        cmd-id-deps}]))))

(deftest test-id-fn
  (let [ctx (-> mock/ctx
                (edd/reg-query :query-1 (fn [ctx query]
                                          (is (= {:realm :realm2}
                                                 (:meta ctx)))
                                          (get
                                           cmd-1-deps
                                           (:id query))))
                (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                      (is (= {:realm :realm2}
                                             (:meta ctx)))
                                      (if (= :1 (:value cmd))
                                        (is (= (:id cmd)
                                               cmd-id-deps))
                                        (is (= (:id cmd)
                                               cmd-id-2)))
                                      {:event-id :event-1
                                       :value    (:value cmd)})
                             :deps {:c1 (fn [cmd] {:query-id :query-1
                                                   :id       (:id cmd)})}
                             :id-fn (fn [ctx cmd]
                                      (get-in ctx [:c1 :id])))
                (edd/reg-fx (fn [ctx [event]]
                              (if (= :1 (:value event))
                                (is (= cmd-id-deps
                                       (:id event)))
                                (is (= cmd-id-2
                                       (:id event))))
                              [])))]
    (let [current-events [{:event-id  :event-1
                           :event-seq 4
                           :value     :2
                           :meta      {}
                           :id        cmd-id-2}]]
      (mock/with-mock-dal
        {:event-store current-events}

        (mock/handle-cmd ctx
                         {:meta     {:realm :realm2}
                          :commands [{:cmd-id :cmd-1
                                      :value  :1
                                      :id     cmd-id-1}
                                     {:cmd-id  :cmd-1
                                      :value   :2
                                      :id      cmd-id-2
                                      :version 4}]})
        (mock/verify-state :event-store [{:event-id  :event-1
                                          :event-seq 1
                                          :value     :1
                                          :meta      {:realm :realm2}
                                          :id        cmd-id-deps}
                                         {:event-id  :event-1
                                          :event-seq 4
                                          :value     :2
                                          :meta      {}
                                          :id        cmd-id-2}
                                         {:event-id  :event-1
                                          :event-seq 5
                                          :value     :2
                                          :meta      {:realm :realm2}
                                          :id        cmd-id-2}]))
      (mock/with-mock-dal
        {:event-store current-events}

        (is (= {:error   :concurrent-modification
                :message "Version mismatch"
                :state   {:current 4
                          :version 6}}
               (mock/handle-cmd ctx
                                {:meta     {:realm :realm2}
                                 :commands [{:cmd-id  :cmd-1
                                             :value   :2
                                             :id      cmd-id-2
                                             :version 6}]})))))))

(deftest test-dependencies-vector
  "Test if context if properly prepared for remote queries"
  (mock/with-mock-dal
    {:dps [{:service        :remote-svc
            :request-id     request-id
            :interaction-id interaction-id
            :query          {:param "Some Value"}
            :resp           {:remote :response}}]}
    (let [meta {:realm :realm5}
          ctx (-> {:meta           meta
                   :request-id     request-id
                   :interaction-id interaction-id}
                  (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                           {:event-id :event-1
                                            :value    (:value cmd)})
                               :deps [:test-value {:query   (fn [cmd]
                                                              {:param (:value cmd)})
                                                   :service :remote-svc}]
                               :id-fn (fn [ctx cmd]
                                        (get-in ctx [:c1 :id]))))
          ctx (cmd/fetch-dependencies-for-command
               ctx
               {:cmd-id :test-cmd
                :id     cmd-id
                :value  "Some Value"})]
      (is (= {:test-value
              {:remote :response}}
             (dissoc ctx :dps)))

      (client/verify-traffic [{:body            (util/to-json
                                                 {:query          {:param "Some Value"}
                                                  :meta           meta
                                                  :request-id     request-id
                                                  :interaction-id interaction-id})
                               :headers         {"X-Authorization" "#mock-id-token"
                                                 "Content-Type"    "application/json"}
                               :method          :post
                               :idle-timeout    10000
                               :connect-timeout 300
                               :url             "https://remote-svc./query"}]))))

(deftest dependant-deps
  (let [current-aggregate {:id      cmd-id-1
                           :version 4
                           :v0      :0}
        ctx (-> mock/ctx
                (edd/reg-event :event-0 (fn [p e]
                                          (assoc
                                           p
                                           :v0 (:value e))))
                (edd/reg-query :get-by-id common/get-by-id)
                (edd/reg-query :query-1 (fn [ctx query]
                                          (is (= {:id       cmd-id-1
                                                  :query-id :query-1
                                                  :c1       current-aggregate}
                                                 query))
                                          {:value :v1}))
                (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                      {:event-id :event-1
                                       :value    (:value cmd)
                                       :c1       (:c1 ctx)
                                       :c2       (:c2 ctx)})
                             :dps [:c1 (fn [cmd] {:query-id :get-by-id
                                                  :id       (:id cmd)})
                                   :c2 (fn [{:keys [c1] :as cmd}]
                                         (is (not= nil
                                                   c1))
                                         {:query-id :query-1
                                          :c1       c1
                                          :id       (:id cmd)})]))]
    (let [current-events [{:event-id  :event-0
                           :event-seq 4
                           :value     :0
                           :meta      {}
                           :id        cmd-id-1}]]
      (mock/with-mock-dal
        {:event-store current-events}

        (mock/handle-cmd ctx {:commands [{:cmd-id :cmd-1
                                          :value  :2
                                          :id     cmd-id-1}]})
        (mock/verify-state :event-store [{:event-id  :event-0
                                          :event-seq 4
                                          :value     :0
                                          :meta      {}
                                          :id        cmd-id-1}
                                         {:event-id  :event-1
                                          :event-seq 5
                                          :value     :2
                                          :meta      {}
                                          :c1        current-aggregate
                                          :c2        {:value :v1}
                                          :id        cmd-id-1}])))))

(deftest duplicate-key-when-chaining-deps
  "When key exist in CMD that is also in DPS then
  we should not handle request"
  (let [ctx (-> mock/ctx
                (edd/reg-query :get-by-id common/get-by-id)
                (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                      [])
                             :dps [:c1 (fn [cmd]
                                         {:query-id :get-by-id
                                          :id       (:id cmd)})
                                   :c2 (fn [cmd]
                                         {:query-id :get-by-id
                                          :id       (:id cmd)})]))]
    (let [current-events [{:event-id  :event-0
                           :event-seq 4
                           :value     :0
                           :meta      {}
                           :id        cmd-id-1}]]
      (mock/with-mock-dal
        {:event-store current-events}
        (is (= {:message "Duplicate key in deps and cmd"
                :error   [:c1]}
               (mock/handle-cmd ctx {:commands [{:cmd-id :cmd-1
                                                 :c1     :something
                                                 :value  :2
                                                 :id     cmd-id-1}]})))))))

(deftest test-encoding
  (is (= "%C3%96VK"
         (URLEncoder/encode "ÖVK", "UTF-8"))))

(deftest serialization-test
  "when we serialize aggregate or search result
  we convert map keys to keywords"
  (let [cmd-id-2 #uuid "0d0532f2-b0df-4a30-99be-ee27783c0f44"
        ctx (-> mock/ctx
                (edd/reg-event :event-1 (fn [p e]
                                          (assoc
                                           p
                                           :v0 (:value e))))
                (edd/reg-query :get-by-id common/get-by-id)
                (edd/reg-cmd :cmd-1 (fn [{:keys [jack]} cmd]
                                      (is (= jack
                                             {:id      cmd-id-2
                                              :v0      {:first-name "Jack"}
                                              :version 1}))
                                      {:event-id :event-1
                                       :value    {"first-name" (:first-name cmd)}})
                             :dps [:jack (fn [_cmd]
                                           {:query-id :get-by-id
                                            :id       cmd-id-2})])
                (edd/reg-cmd :cmd-2 (fn [{:keys [aggregate]} _cmd]
                                      (is (= aggregate
                                             {:id      cmd-id-1
                                              :v0      {:first-name "Edd-1"}
                                              :version 1}))
                                      [])
                             :dps [:aggregate (fn [cmd] {:query-id :get-by-id
                                                         :id       (:id cmd)})]))]

    (mock/with-mock-dal
      {:aggregate-store [{:id      cmd-id-2
                          :v0      {"first-name" "Jack"}
                          :version 1}]}
      (mock/apply-cmd ctx {:commands [{:cmd-id     :cmd-1
                                       "first-name" "Edd-1"
                                       :id         cmd-id-1}]})
      (mock/verify-state :aggregate-store [{:id      cmd-id-2
                                            :v0      {:first-name "Jack"}
                                            :version 1}
                                           {:id      cmd-id-1
                                            :v0      {:first-name "Edd-1"}
                                            :version 1}])
      (is (= {:id      cmd-id-1
              :v0      {:first-name "Edd-1"}
              :version 1}
             (mock/query ctx {:query-id :get-by-id
                              :id       cmd-id-1})))
      (mock/verify-state :event-store [{:event-id  :event-1
                                        :event-seq 1
                                        :id        cmd-id-1
                                        :meta      {}
                                        :value     {:first-name "Edd-1"}}])
      (mock/apply-cmd ctx {:commands [{:cmd-id     :cmd-2
                                       :first-name "Edd-2"
                                       :id         cmd-id-1}]}))))