(ns edd.test.fixture.dal-test
  (:require [edd.dal :as dal]
            [edd.test.fixture.dal :refer :all]
            [edd.common :as common]
            [edd.core :as edd]
            [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [lambda.util :as util]))

(deftest when-store-and-load-events-then-ok
  (with-mock-dal (dal/store-event {} {} {:id 1 :info "info"})
    (verify-state [{:id 1 :info "info"}] :event-store)
    (let [events (dal/get-events {} 1)]
      (is (= [{:id 1 :info "info"}]
             events)))))

(deftest when-update-aggregate-then-ok
  (with-mock-dal (dal/update-aggregate {} {:id 1 :payload "payload"})
    (verify-state [{:id      1
                    :payload "payload"}] :aggregate-store)
    (dal/update-aggregate {} {:id 1 :payload "payload2"})
    (verify-state [{:id      1
                    :payload "payload2"}] :aggregate-store)))

(deftest when-query-aggregate-with-unknown-condition-then-return-nothing
  (with-mock-dal (dal/update-aggregate {} {:id 1 :payload "payload"})
    (dal/update-aggregate {} {:id 2 :payload "payload"})
    (dal/update-aggregate {} {:id 3 :payload "pa2"})
    (is (= [{:id 3 :payload "pa2"}]
           (dal/simple-search {} {:id 3})))
    (is (= []
           (dal/simple-search {} {:id 4})))))

(deftest when-store-sequence-then-ok
  (with-mock-dal (dal/store-sequence {} {:id "id1"})
    (dal/store-sequence {} {:id "id2"})
    (verify-state [{:id    "id1"
                    :value 1}
                   {:id    "id2"
                    :value 2}] :sequence-store)
    (is (= 1
           (common/get-sequence-number-for-id {} {:id "id1"})))
    (is (= 2
           (common/get-sequence-number-for-id {} {:id "id2"})))
    (is (= "id1"
           (common/get-id-for-sequence-number {} {:value 1})))
    (is (= "id2"
           (common/get-id-for-sequence-number {} {:value 2})))))

(deftest when-sequnce-null-then-fail
  (with-mock-dal
    (is (thrown? AssertionError
                 (dal/store-sequence {} "id2")))
    (is (thrown? AssertionError
                 (dal/query-id-for-sequence-number {} "id2")))
    (is (thrown? AssertionError
                 (dal/query-sequence-number-for-id {} "id2")))))

(deftest when-sequence-exists-then-exception
  (with-mock-dal (dal/store-sequence {} {:id 1})
    (is (thrown? RuntimeException
                 (dal/store-sequence
                  {}
                  {:id 1})))
    (verify-state [{:id    1
                    :value 1}] :sequence-store)))

(deftest when-store-identity-then-ok
  (with-mock-dal
    (dal/store-identity {} {:identity 1})
    (dal/store-identity {} {:identity 2})
    (verify-state [{:identity 1} {:identity 2}] :identity-store)))

(deftest when-identity-exists-then-exception
  (with-mock-dal
    (dal/store-identity {} {:identity 1})
    (is (thrown? RuntimeException
                 (dal/store-identity
                  {}
                  {:identity 1})))
    (verify-state [{:identity 1}] :identity-store)))

(deftest when-store-command-then-ok
  (with-mock-dal
    (dal/store-cmd {} {:service "test-service" :payload "payload"})
    (verify-state [{:service "test-service"
                    :payload "payload"}] :command-store)))

(deftest when-identity-exists-then-id-for-aggregate-can-be-fetched
  (with-mock-dal
    (dal/store-identity {} {:identity 1
                            :id       2})
    (verify-state [{:identity 1
                    :id       2}] :identity-store)
    (is (= 2
           (common/get-aggregate-id-by-identity {} 1)))))

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
    (dal/store-event {} "a" (first events))
    (dal/store-event {} "a" (second events))
    (dal/store-event {} "a" {:event-id  :name
                             :name      "Bla"
                             :event-seq 3
                             :id        4})
    (verify-state (conj events {:event-id  :name
                                :name      "Bla"
                                :event-seq 3
                                :id        4}) :event-store)
    (is (= {:name "Your"
            :id   2}
           (common/get-by-id
            (edd/reg-event
             {}
             :name (fn [state event]
                     {:name (:name event)
                      :id   (:id event)}))
            {:id 2})))))

(deftest when-no-result-by-id-return-nil
  (with-mock-dal
    (dal/store-event {} "a" (first events))
    (verify-state [(first events)] :event-store)
    (is (= nil
           (common/get-by-id {} {:id 5})))))

(deftest verify-predefined-state
  (with-mock-dal
    {:event-store [{:event-id :e1}]}
    (verify-state [{:event-id :e1}] :event-store)
    (verify-state [] :command-store)
    (dal/store-event {} "a" (first events))
    (verify-state [{:event-id :e1} (first events)] :event-store)
    (is (= nil
           (common/get-by-id {} {:id 5})))))

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
  (with-mock-dal (dal/update-aggregate {} v1)
    (dal/update-aggregate {} v2)
    (let [resp
          (dal/simple-search
           {}
           {:query-id :id1
            :at1      "val2-1"
            :at2      {:at4 "val2-4"}})]
      (is (= v2
             (first resp))))))

(def realm-id "")
(def e1
  {:id        1
   :event-seq 1})

(def e2
  {:id        1
   :event-seq 2})

(deftest test-event-seq-and-event-order
  (with-mock-dal
    (dal/store-event {} realm-id e1)
    (dal/store-event {} realm-id e2)
    (is (= 2 (dal/get-max-event-seq {} 1)))
    (is (= 0 (dal/get-max-event-seq {} 3)))
    (is (= [e1 e2]
           (dal/get-events {} 1)))))

(deftest test-reverse-event-order
  (with-mock-dal
    (dal/store-event {} realm-id e1)
    (dal/store-event {} realm-id e2)
    (is (= [e1 e2]
           (dal/get-events {} 1)))))

(deftest test-reverse-event-order
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
         (util/to-edn (dal/create-simple-query {:first "zeko"
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
           (dal/simple-search {:service-name ""} {})))))
