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
            [edd.test.fixture.dal :as mock])
  (:import (java.net URLEncoder)
           (clojure.lang ExceptionInfo)))

(def cmd-id #uuid "11111eeb-e677-4d73-a10a-1d08b45fe4dd")

(def request-id #uuid "22222eeb-e677-4d73-a10a-1d08b45fe4dd")
(def interaction-id #uuid "33333eeb-e677-4d73-a10a-1d08b45fe4dd")

(deftest test-prepare-context-for-command-with-remote-query
  "Test if context if properly prepared for remote queries"
  (mock/with-mock-dal
    {:dps [{:service        :remote-svc
            :request-id     request-id
            :interaction-id interaction-id
            :query          {:param "Some Value"}
            :resp           {:remote :response}}]}
    (let [ctx (cmd/resolve-dependencies-to-context
               (-> {:dps            {:test-cmd
                                     {:test-value {:query   (fn [cmd]
                                                              {:param (:value cmd)})
                                                   :service :remote-svc}}}
                    :commands       [{:cmd-id :test-cmd
                                      :id     cmd-id
                                      :value  "Some Value"}]
                    :request-id     request-id
                    :interaction-id interaction-id}
                   (event-store/register)))]
      (is (= {:request-id     request-id
              :interaction-id interaction-id
              :dps-resolved   {:test-value
                               {:remote :response}}}
             (dissoc ctx :dps :commands :event-store)))

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

(def ctx (-> mock/ctx
             (edd/reg-query :query-1 (fn [ctx query]
                                       (get
                                        cmd-1-deps
                                        (:id query))))
             (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                   {:event-id :event-1
                                    :value    (:value cmd)})
                          :dps {:c1 (fn [cmd] {:query-id :query-1
                                               :id       (:id cmd)})
                                :c2 (fn [_] nil)}
                          :id-fn (fn [ctx cmd]
                                   (get-in ctx [:c1 :id])))))

#_(deftest if-fn-to-context-test
    (is (= [cmd-1
            {:cmd-id      :cmd-1
             :value       :2
             :id          cmd-id-deps
             :original-id cmd-id-2}]
           (:commands
            (cmd/resolve-commands-id-fn (assoc ctx
                                               :dps-resolved [{} {:c1 {:id cmd-id-deps}}]
                                               :commands [cmd-1
                                                          {:cmd-id :cmd-1
                                                           :value  :2
                                                           :id     cmd-id-2}]))))))

(deftest test-id-fn-integration
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
                                      :id        cmd-id-deps}])))

(deftest test-id-fn
  (let [ctx (-> mock/ctx
                (edd/reg-query :query-1 (fn [ctx query]
                                          (get
                                           cmd-1-deps
                                           (:id query))))
                (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                      (if (= :1 (:value cmd))
                                        (is (= (:id cmd)
                                               cmd-id-deps))
                                        (is (= (:id cmd)
                                               cmd-id-2)))
                                      {:event-id :event-1
                                       :value    (:value cmd)})
                             :dps {:c1 (fn [cmd] {:query-id :query-1
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
                         {:commands [{:cmd-id :cmd-1
                                      :value  :1
                                      :id     cmd-id-1}
                                     {:cmd-id  :cmd-1
                                      :value   :2
                                      :id      cmd-id-2
                                      :version 4}]})
        (mock/verify-state :event-store [{:event-id  :event-1
                                          :event-seq 1
                                          :value     :1
                                          :meta      {}
                                          :id        cmd-id-deps}
                                         {:event-id  :event-1
                                          :event-seq 4
                                          :value     :2
                                          :meta      {}
                                          :id        cmd-id-2}
                                         {:event-id  :event-1
                                          :event-seq 5
                                          :value     :2
                                          :meta      {}
                                          :id        cmd-id-2}]))
      (mock/with-mock-dal
        {:event-store current-events}

        (is (thrown?
             ExceptionInfo
             (mock/handle-cmd ctx
                              {:commands [{:cmd-id  :cmd-1
                                           :value   :2
                                           :id      cmd-id-2
                                           :version 6}]})))))))

(deftest test-dependevies-vector
  "Test if context if properly prepared for remote queries"
  (mock/with-mock-dal
    {:dps [{:service        :remote-svc
            :request-id     request-id
            :interaction-id interaction-id
            :query          {:param "Some Value"}
            :resp           {:remote :response}}]}
    (let [ctx (cmd/fetch-dependencies-for-command
               {:dps            {:test-cmd
                                 [:test-value {:query   (fn [cmd]
                                                          {:param (:value cmd)})
                                               :service :remote-svc}]}
                :request-id     request-id
                :interaction-id interaction-id}
               {:cmd-id :test-cmd
                :id     cmd-id
                :value  "Some Value"})]
      (is (= {:request-id     request-id
              :interaction-id interaction-id
              :dps-resolved   {:test-value
                               {:remote :response}}}
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
(deftest test-encoding
  (is (= "%C3%96VK"
         (URLEncoder/encode "ÖVK", "UTF-8"))))
