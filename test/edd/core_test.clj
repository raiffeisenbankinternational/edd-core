(ns edd.core-test
  (:require [clojure.tools.logging :as log]
            [edd.core :as edd]
            [edd.dal :as dal]
            [edd.search :as search]
            [edd.el.event :as event]
            [edd.el.cmd :as cmd]
            [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.core :as core]
            [lambda.api-test :as api]
            [lambda.filters :as fl]
            [lambda.util :as util]
            [edd.common :as common]
            [next.jdbc :as jdbc]
            [lambda.test.fixture.core :refer [mock-core]]
            [lambda.test.fixture.client :refer [verify-traffic-json]]
            [edd.el.query :as query]
            [lambda.s3-test :as s3]
            [edd.memory.view-store :as view-store]
            [edd.memory.event-store :as event-store]
            [edd.test.fixture.dal :as mock]
            [edd.postgres.event-store :as pg]
            [lambda.uuid :as uuid]))

(defn dummy-command-handler
  [ctx cmd]
  (log/info "Dummy" cmd)
  {:event-id :dummy-event
   :id       (:id cmd)
   :handled  true})

(defn dummy-command-handler-2
  [ctx cmd]
  (log/info "Dummy" cmd)
  [{:event-id :dummy-event-1
    :id       (:id cmd)
    :handled  true}
   {:event-id :dummy-event-2
    :id       (:id cmd)
    :handled  true}])

(defn dummy-command-handler-3
  [ctx cmd]
  (log/info "Dummy" cmd)
  [{:event-id :dummy-event-3
    :id       (:id cmd)
    :handled  true}])

(def fx-id #uuid "22222111-1111-1111-1111-111111111111")
(def fx-command
  {:commands [{:cmd-id :fx-command
               :id     #uuid "22222111-1111-1111-1111-111111111111"}]
   :service  :local-test})

(defn prepare
  [ctx]
  (-> ctx
      (event-store/register)
      (view-store/register)
      (assoc :service-name :local-test)
      (edd/reg-cmd :dummy-cmd dummy-command-handler)
      (edd/reg-fx (fn [ctx events]
                    (if (some #(= (:event-id %)
                                  :dummy-event)
                              events)
                      fx-command
                      [])))
      (edd/reg-cmd :dummy-cmd-2
                   dummy-command-handler-2
                   :dps {:test-dps (fn [cmd] {:query-id :test-query})})
      (edd/reg-cmd :dummy-cmd-3
                   dummy-command-handler-3
                   :dps {:test-dps (fn [cmd] {:query-id :test-query})}
                   :id-fn (fn [dps cmd] (get-in dps [:test-dps :id])))
      (edd/reg-cmd :object-uploaded dummy-command-handler)
      (edd/reg-cmd :error-cmd (fn [ctx cmd] {:error "Some error"}))
      (edd/reg-query :get-by-id (fn [ctx query]
                                  (common/get-by-id (assoc ctx :id (:id query)))))
      (edd/reg-event :e7 (fn [ctx event] {:name (:event-id event)}))))

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))
(def cmd-id-2 (uuid/parse "222222-2222-2222-2222-222222222222"))
(def cmd-id-3 (uuid/parse "333333-3333-3333-3333-333333333333"))
(def dps-id (uuid/parse "dddddd-dddd-dddd-dddd-dddddddddddd"))

(def multi-command-request
  {:commands [{:cmd-id :dummy-cmd
               :id     cmd-id}
              {:cmd-id :dummy-cmd-2
               :id     cmd-id-2}
              {:cmd-id :dummy-cmd-3
               :id     cmd-id-3}]})

#_(deftest test-command-id-resolution
    (let [resp (cmd/resolve-commands-id-fn
                (merge
                 multi-command-request
                 {:id-fn {:dummy-cmd-2
                          (fn [ctx cmd] dps-id)}}))]
      (is (= {:commands [{:cmd-id :dummy-cmd
                          :id     #uuid "00111111-1111-1111-1111-111111111111"}
                         {:cmd-id      :dummy-cmd-2
                          :id          #uuid "00dddddd-dddd-dddd-dddd-dddddddddddd"
                          :original-id #uuid "00222222-2222-2222-2222-222222222222"}
                         {:cmd-id :dummy-cmd-3
                          :id     #uuid "00333333-3333-3333-3333-333333333333"}]}
             (select-keys resp [:commands])))))

(deftest handler-builder-test
  "Test if id-fn works correctly together with event seq. This test does multiple things. Sorry!!"
  (mock/with-mock-dal
    (let [resp (mock/handle-cmd
                (prepare {})
                {:commands [{:cmd-id :error-cmd
                             :id     cmd-id}]})]
      (is (= {:error [{:error "Some error"
                       :id    cmd-id}]}
             (select-keys (dissoc resp :meta) [:error]))))))

(deftest test-error-response
  "Test if id-fn works correctly together with event seq. This test does multiple things. Sorry!!"
  (with-redefs [event-store/get-max-event-seq-impl (fn [{:keys [id]}]
                                                     (get {cmd-id   21
                                                           dps-id   31
                                                           cmd-id-2 5
                                                           cmd-id-3 9}
                                                          id))]
    (mock/with-mock-dal
      (let [resp (mock/handle-cmd
                  (-> (prepare {})
                      (edd/reg-query :test-query (fn [_ _] {:id dps-id})))
                  multi-command-request)]
        (mock/verify-state :event-store [{:event-id  :dummy-event-1
                                          :handled   true
                                          :event-seq 6
                                          :id        cmd-id-2}
                                         {:event-id  :dummy-event-2
                                          :handled   true
                                          :event-seq 7
                                          :id        cmd-id-2}
                                         {:event-id  :dummy-event
                                          :handled   true
                                          :event-seq 22
                                          :id        cmd-id}
                                         {:event-id  :dummy-event-3
                                          :handled   true
                                          :event-seq 32
                                          :id        dps-id}])
        (is (= {:effects    [{:cmd-id       :fx-command
                              :id           #uuid "22222111-1111-1111-1111-111111111111"
                              :service-name :local-test}]
                :events     4
                :identities 0
                :meta       [{:dummy-cmd {:id cmd-id}}
                             {:dummy-cmd-2 {:id cmd-id-2}}
                             {:dummy-cmd-3 {:id dps-id}}]
                :sequences  0
                :success    true}
               resp))))))

(def request-id #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491")
(def interaction-id #uuid "2222b7b5-9f50-4dc4-86d1-2e4fe1f6d491")

(deftest test-s3-bucket-request
  (mock/with-mock-dal
    (with-redefs [aws/create-date (fn [] "20200426T061823Z")
                  uuid/gen (fn [] request-id)]
      (mock-core
       :invocations [(s3/records "test/key")]
       :requests [{:get  "https://s3.eu-central-1.amazonaws.com/example-bucket/test/key"
                   :body (char-array "Of something")}]
       (core/start
        (prepare {})
        edd/handler
        :filters [fl/from-bucket])
       (verify-traffic-json (cons
                             {:body   {:interaction-id request-id
                                       :request-id     request-id
                                       :result         {:effects    [{:cmd-id       :fx-command
                                                                      :id           #uuid "22222111-1111-1111-1111-111111111111"
                                                                      :service-name :local-test}]
                                                        :events     1
                                                        :identities 0
                                                        :meta       [{:object-uploaded {:id #uuid "1111b7b5-9f50-4dc4-86d1-2e4fe1f6d491"}}]
                                                        :sequences  0
                                                        :success    true}}
                              :method :post
                              :url    "http://mock/2018-06-01/runtime/invocation/0/response"}
                             s3/base-requests))))))

(deftest test-api-request-with-fx
  "Test that user is added to events and summary is properly returned"
  (mock/with-mock-dal
    (mock-core
     :invocations [(api/api-request
                    {:commands       [{:cmd-id :dummy-cmd,
                                       :bla    "ble",
                                       :id     cmd-id}],
                     :user           {:selected-role :group-2}
                     :request-id     request-id,
                     :interaction-id interaction-id})]

     :requests [{:get  "https://s3.eu-central-1.amazonaws.com/example-bucket/test/key"
                 :body (char-array "Of something")}]
     (core/start
      (prepare {})
      edd/handler
      :filters [fl/from-api]
      :post-filter fl/to-api)
     (verify-traffic-json [{:body   {:body            (util/to-json
                                                       {:result         {:success    true
                                                                         :effects    [{:id           fx-id
                                                                                       :cmd-id       :fx-command
                                                                                       :service-name :local-test}]
                                                                         :events     1
                                                                         :meta       [{:dummy-cmd {:id cmd-id}}]
                                                                         :identities 0
                                                                         :sequences  0}
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
                            :url     "http://mock/2018-06-01/runtime/invocation/next"}]))))

(def apply-ctx
  {:def-apply {:event-1 (fn [p v]
                          (assoc p :e1 v))
               :event-2 (fn [p v]
                          (assoc p :e2 v))}})

(deftest test-apply
  (let [agg (event/get-current-state
             (assoc apply-ctx
                    :events [{:event-id  :event-1
                              :id        cmd-id
                              :event-seq 1
                              :k1        "a"}
                             {:event-id  :event-2
                              :id        cmd-id
                              :event-seq 2
                              :k2        "b"}]
                    :id "ag1"))]
    (is (= {:aggregate {:id      cmd-id
                        :version 2
                        :e1      {:event-id  :event-1,
                                  :k1        "a"
                                  :event-seq 1
                                  :id        cmd-id},
                        :e2      {:event-id  :event-2
                                  :k2        "b"
                                  :event-seq 2
                                  :id        cmd-id}}}
           (select-keys agg [:aggregate])))))

(deftest test-get-by-id

  (with-redefs [dal/get-events (fn [_]
                                 [{:event-id :e7
                                   :event-seq 1}])
                search/simple-search (fn [ctx] ctx)]
    (let [agg (query/handle-query
               (prepare {})
               {:query
                {:query-id :get-by-id
                 :id       "bla"}})]
      (is (= {:name :e7
              :version 1
              :id nil}
             (:aggregate agg))))))

(deftest test-query-with-invalid-schema

  (with-redefs [dal/get-events (fn [ctx id]
                                 [{:event-id :e7}]) Ŧ]
    (let [agg (query/handle-query
               (prepare {})
               {:query
                {:query-id "get-by-id"
                 :id       "bla"}})]
      (is (:error agg)))))

(deftest test-wrap-global-not-list
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  {:a :b})]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  [{:a :b}])]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global-commands
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  [{:service  "a"
                                    :commands [{:a :b}]}])]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global-commands-in-vector
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  {:service  "a"
                                   :commands [{:a :b}]})]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-wrap-global-commands-as-list
  (let [result (cmd/wrap-commands {:service-name "a"}
                                  '({:service  "a"
                                     :commands [{:a :b}]}))]
    (is (= [{:service  "a"
             :commands [{:a :b}]}]
           result))))

(deftest test-metadata-when-result-vector
  (is (= {:request-id     request-id
          :interaction-id interaction-id
          :result         [:a]}
         (:resp
          (edd/prepare-response {:request-id     request-id
                                 :interaction-id interaction-id
                                 :resp           [:a]})))))

(deftest test-metadata-when-result-error
  (is (= {:request-id     request-id
          :interaction-id interaction-id
          :error          "Some error"}
         (:resp
          (edd/prepare-response {:request-id     request-id
                                 :interaction-id interaction-id
                                 :resp           {:error "Some error"}})))))

(deftest test-metadata-when-result-map
  (is (= {:request-id     request-id
          :interaction-id interaction-id
          :result         {:value :a}}
         (:resp
          (edd/prepare-response {:request-id     request-id
                                 :interaction-id interaction-id
                                 :resp           {:value :a}})))))
