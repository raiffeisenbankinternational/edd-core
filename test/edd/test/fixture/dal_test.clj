(ns edd.test.fixture.dal-test
  (:require [edd.test.fixture.dal :refer [verify-state
                                          ctx
                                          with-mock-dal
                                          pop-state
                                          peek-state
                                          verify-state-fn]]
            [edd.common :as common]
            [edd.search :as search]
            [edd.core :as edd]
            [edd.dal :as event-store]
            [edd.memory.event-store :as dal]
            [edd.elastic.view-store :as elastic-view]
            [edd.search :as view-store]
            [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [edd.elastic.view-store :as elastic]
            [lambda.util :as util]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]))

(deftest when-store-and-load-events-then-ok
  (with-mock-dal
    (dal/store-event {:id 1 :info "info"})
    (verify-state [{:id 1 :info "info"}] :event-store)
    (let [events (event-store/get-events (assoc ctx
                                                :id 1))]
      (is (= [{:id 1 :info "info"}]
             events)))))

(deftest when-update-aggregate-then-ok
  (with-mock-dal (view-store/update-aggregate (assoc ctx
                                                     :aggregate {:id 1 :payload "payload"}))
    (verify-state [{:id      1
                    :payload "payload"}] :aggregate-store)
    (view-store/update-aggregate (assoc ctx
                                        :aggregate {:id 1 :payload "payload2"}))
    (verify-state [{:id      1
                    :payload "payload2"}] :aggregate-store)))

(deftest when-query-aggregate-with-unknown-condition-then-return-nothing
  (with-mock-dal (view-store/update-aggregate (assoc ctx
                                                     :aggregate {:id 1 :payload "payload"}))
    (view-store/update-aggregate (assoc ctx
                                        :aggregate {:id 2 :payload "payload"}))
    (view-store/update-aggregate (assoc ctx
                                        :aggregate {:id 3 :payload "pa2"}))
    (is (= [{:id 3 :payload "pa2"}]
           (view-store/simple-search (assoc ctx
                                            :query {:id 3}))))
    (is (= []
           (view-store/simple-search (assoc ctx
                                            :query {:id 4}))))))

(deftest when-store-sequence-then-ok
  (with-mock-dal (dal/store-sequence {:id "id1"})
    (dal/store-sequence {:id "id2"})
    (verify-state [{:id    "id1"
                    :value 1}
                   {:id    "id2"
                    :value 2}] :sequence-store)
    (is (= 1
           (event-store/get-sequence-number-for-id (assoc ctx
                                                          :id "id1"))))
    (is (= 2
           (event-store/get-sequence-number-for-id (assoc ctx
                                                          :id "id2"))))
    (is (= "id1"
           (event-store/get-id-for-sequence-number (assoc ctx
                                                          :sequence 1))))
    (is (= "id2"
           (event-store/get-id-for-sequence-number (assoc ctx
                                                          :sequence 2))))))

(deftest when-sequnce-null-then-fail
  (with-mock-dal
    (is (thrown? AssertionError
                 (dal/store-sequence {:id nil})))

    (dal/store-sequence {:id "id2"})
    (is (= "id2"
           (event-store/get-id-for-sequence-number (assoc ctx
                                                          :sequence 1))))
    (is (thrown? AssertionError
                 (event-store/get-id-for-sequence-number (assoc ctx
                                                                :sequence nil))))
    (is (thrown? AssertionError
                 (event-store/get-sequence-number-for-id (assoc ctx
                                                                :id nil))))))

(deftest when-sequence-exists-then-exception
  (with-mock-dal (dal/store-sequence {:id 1})
    (is (thrown? RuntimeException
                 (dal/store-sequence
                  {:id 1})))
    (verify-state [{:id    1
                    :value 1}] :sequence-store)))

(deftest when-store-identity-then-ok
  (with-mock-dal
    (dal/store-identity {:identity 1
                         :id       1})
    (dal/store-identity {:identity 2
                         :id       2})
    (verify-state [{:identity 1
                    :id       1}
                   {:identity 2
                    :id       2}] :identity-store)))

(deftest when-identity-exists-then-exception
  (with-mock-dal
    (dal/store-identity {:identity 1})
    (is (thrown? RuntimeException
                 (dal/store-identity
                  {:identity 1})))
    (verify-state [{:identity 1}] :identity-store)))

(deftest when-store-command-then-ok
  (with-mock-dal
    (dal/store-command {:service "test-service" :payload "payload"})
    (verify-state [{:service "test-service"
                    :payload "payload"}] :command-store)))

(deftest when-identity-exists-then-id-for-aggregate-can-be-fetched
  (with-mock-dal
    (dal/store-identity {:identity 1
                         :id       2})
    (verify-state [{:identity 1
                    :id       2}] :identity-store)
    (is (= 2
           (event-store/get-aggregate-id-by-identity (assoc ctx
                                                            :identity 1))))))

(def events
  [{:event-id  :name
    :name      "Me"
    :event-seq 1
    :id        2}
   {:event-id  :name
    :name      "Your"
    :event-seq 2
    :id        2}])

(deftest test-that-events-can-be-fetched-by-aggregate-id
  (with-mock-dal
    (dal/store-event (first events))
    (dal/store-event (second events))
    (dal/store-event {:event-id  :name
                      :name      "Bla"
                      :event-seq 3
                      :id        4})
    (verify-state (conj events {:event-id  :name
                                :name      "Bla"
                                :event-seq 3
                                :id        4}) :event-store)
    (is (= {:name    "Your"
            :version 2
            :id      2}
           (-> ctx
               (edd/reg-event
                :name (fn [state event]
                        {:name (:name event)
                         :id   (:id event)}))
               (assoc :id 2)
               (common/get-by-id)
               :aggregate)))))

(deftest when-no-result-by-id-return-nil
  (with-mock-dal
    (dal/store-event (first events))
    (verify-state [(first events)] :event-store)
    (is (= :no-events-found
           (:error
            (common/get-by-id (assoc ctx :id 5)))))))

(deftest verify-predefined-state
  (with-mock-dal
    {:event-store [{:event-id :e1}]}
    (verify-state [{:event-id :e1}] :event-store)
    (verify-state [] :command-store)
    (dal/store-event (first events))
    (verify-state [{:event-id :e1} (first events)] :event-store)
    (is (= :no-events-found
           (:error
            (common/get-by-id (assoc ctx :id 5)))))))

(def v1
  {:id  1
   :at1 "val1-1"
   :at2 {:at3 "val1-3"
         :at4 "val1-4"}})

(def v2
  {:id  2
   :at1 "val2-1"
   :at2 {:at3 "val2-3"
         :at4 "val2-4"}})

(deftest test-simple-search-result
  (with-mock-dal (view-store/update-aggregate (assoc ctx
                                                     :aggregate v1))
    (view-store/update-aggregate (assoc ctx
                                        :aggregate v2))
    (let [resp
          (view-store/simple-search
           (assoc ctx
                  :query {:query-id :id1
                          :at1      "val2-1"
                          :at2      {:at4 "val2-4"}}))]
      (is (= v2
             (first resp))))))

(def e1
  {:id        1
   :event-seq 1})

(def e2
  {:id        1
   :event-seq 2})

(deftest test-event-seq-and-event-order
  (with-mock-dal
    (dal/store-event e1)
    (dal/store-event e2)
    (is (= 2 (event-store/get-max-event-seq (assoc ctx :id 1))))
    (is (= 0 (event-store/get-max-event-seq (assoc ctx :id 3))))
    (is (= [e1 e2]
           (event-store/get-events (assoc ctx :id 1))))))

(deftest test-reverse-event-order-1
  (with-mock-dal
    (dal/store-event e1)
    (dal/store-event e2)
    (is (= [e1 e2]
           (event-store/get-events (assoc ctx
                                          :id 1))))))

(deftest test-reverse-event-order-2
  (with-mock-dal
    {:command-store [{:cmd 1}]
     :event-store   [{:event :bla}]}
    (is (= {:aggregate-store []
            :command-store   [{:cmd 1}]
            :event-store     [{:event :bla}]
            :identity-store  []
            :sequence-store  []}
           (peek-state)))

    (is (= [{:cmd 1}]
           (peek-state :command-store)))
    (is (= [{:event :bla}]
           (peek-state :event-store)))

    (is (= [{:event :bla}]
           (pop-state :event-store)))

    (is (= {:aggregate-store []
            :command-store   [{:cmd 1}]
            :event-store     []
            :identity-store  []
            :sequence-store  []}
           (peek-state)))))

(deftest test-simple-query
  (is (= {:size  600
          :query {:bool
                  {:must
                   [{:term {:first.keyword "zeko"}}
                    {:term {:last.two.keyword "d"}}]}}}
         (util/to-edn (elastic/create-simple-query {:first "zeko"
                                                    :last  {:two "d"}})))))
(def elk-objects
  [{:attrs   {:cocunut           "222996"
              :cognos            "ABADEK"
              :company           "Abade Immobilienleasing GmbH & Co Projekt Lauterbach KG, 65760 Eschborn (DE)"
              :ifrs              "Group Corporates & Markets"
              :int               "Financial Institution acc. CRR /BWG"
              :iso               "DE"
              :key-account       "Inna Shala"
              :oenb-id-number    "8324301"
              :parent-id         "#74094776-3cd9-4b48-9deb-4c38b3c96435"
              :parent-short-name "RLGMBH GROUP"
              :rbi-group-cocunut "222996"
              :rbi-knr           ""
              :reporting-ccy     "EUR"
              :share-in-%        "6,00%"
              :short-code        "ABADEKG"
              :short-name        "ABADEKG"
              :sorter            "Leasing Austria"
              :type              ":booking-company"}
    :cocunut "222996"
    :id      "#e1a1e96f-93bb-4fdd-9605-ef2b38c1c458"
    :parents ["#74094776-3cd9-4b48-9deb-4c38b3c96435"]
    :state   ":detached"}
   {:attrs   {:cocunut           "188269"
              :cognos            "ABADE"
              :company           "Abade Immobilienleasing GmbH, 65760 Eschborn (DE)"
              :ifrs              "Group Corporates & Markets"
              :int               "Financial Institution acc. CRR /BWG"
              :iso               "DE"
              :key-account       "Inna Shala"
              :oenb-id-number    "8142262"
              :parent-id         "#74094776-3cd9-4b48-9deb-4c38b3c96435"
              :parent-short-name "RLGMBH GROUP"
              :rbi-group-cocunut "188269"
              :rbi-knr           ""
              :reporting-ccy     "EUR"
              :share-in-%        "100,00%"
              :short-code        "ABADE"
              :short-name        "ABADE"
              :sorter            "Leasing Austria"
              :type              ":booking-company"}
    :cocunut "188269"
    :id      "#7c30b6a3-2816-4378-8ed9-0b73b61012d4"
    :parents ["#74094776-3cd9-4b48-9deb-4c38b3c96435"]
    :state   ":detached"}])

(def elk-response
  (future {:opts    {:body      (util/to-json {:size 600
                                               :query
                                               {:bool
                                                {:must
                                                 [{:match
                                                   {:attrs.type ":booking-company"}}]}}}),
                     :headers   {"Content-Type" "application/json"
                                 "X-Amz-Date"   "20200818T113334Z"},
                     :timeout   5000,
                     :keepalive 300000,
                     :method    :post
                     :url       "https://vpc-mock.eu-central-1.es.amazonaws.com/glms_risk_taker_svc/_search"}
           :body    (util/to-json {:took      42,
                                   :timed_out false,
                                   :_shards
                                   {:total      5,
                                    :successful 5,
                                    :skipped    0,
                                    :failed     0},
                                   :hits
                                   {:total     {:value 2, :relation "eq"},
                                    :max_score 0.09304003,
                                    :hits
                                    [{:_index  "glms_risk_taker_svc",
                                      :_type   "_doc",
                                      :_id
                                      "e1a1e96f-93bb-4fdd-9605-ef2b38c1c458",
                                      :_score  0.09304003,
                                      :_source (first elk-objects)}
                                     {:_index  "glms_risk_taker_svc",
                                      :_type   "_doc",
                                      :_id
                                      "7c30b6a3-2816-4378-8ed9-0b73b61012d4",
                                      :_score  0.09304003,
                                      :_source (second elk-objects)}]}})
           :headers {:access-control-allow-origin "*"
                     :connection                  "keep-alive"
                     :content-encoding            "gzip"},
           :status  200}))

(deftest elastic-search
  (with-redefs [http/post (fn [url req] elk-response)]
    (is (= elk-objects
           (search/simple-search (-> {}
                                     (elastic-view/register)
                                     (assoc :service-name "test"
                                            :query {})))))))

(deftest when-identity-exists-then-exception
  (with-mock-dal
    (dal/store-identity {:id       "id1"
                         :identity 1})
    (is (thrown? RuntimeException
                 (dal/store-identity
                  {:id       "id1"
                   :identity 2})))
    (verify-state [{:id       "id1"
                    :identity 1}] :identity-store)))

(deftest test-identity-generation
  (with-mock-dal {:identities {"id1" "some-id"}}
    (is (= "some-id"
           (common/create-identity "id1")))))

(deftest verify-state-fn-ok-test
  (with-mock-dal {:aggregate-store [{:a "a"
                                     :b "b"}
                                    {:a "c"
                                     :d "d"}]}
    (verify-state-fn :aggregate-store
                     #(dissoc % :a)
                     [{:b "b"}
                      {:d "d"}])))

(deftest apply-cmd-test
  (with-mock-dal
    (let [id (uuid/gen)
          ctx (-> mock/ctx
                  (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                           {:event-id :1})))]
      (mock/apply-cmd ctx {:cmd-id :test-cmd
                           :id     id})
      (is (= {:effects    []
              :events     1
              :identities 0
              :meta       [{:test-cmd {:id id}}]
              :sequences  0
              :success    true}
             (mock/handle-cmd ctx {:cmd-id :test-cmd
                                   :id     id}))))))
