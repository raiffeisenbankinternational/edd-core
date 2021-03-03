(ns edd.apply-test
  (:require [clojure.test :refer :all]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [edd.el.event :as event]

            [edd.core :as edd]
            [edd.test.fixture.dal :as mock]
            [edd.memory.event-store :as event-store]
            [edd.memory.view-store :as view-store]
            [edd.dal :as dal]))

(def agg-id (uuid/parse "0000bf24-c357-4ee2-ae1e-6ce22c90c183"))

(def req-id (uuid/parse "1111bf24-c357-4ee2-ae1e-6ce22c90c183"))
(def int-id (uuid/parse "2222bf24-c357-4ee2-ae1e-6ce22c90c183"))

(def req
  {:Records
   [{:md5OfBody
                        "fff479f1b3e7bae94d4fbb22f1b2cce0",
     :eventSourceARN
                        "arn:aws:sqs:eu-central-1:11111111111:test-evets-queue",
     :awsRegion         "eu-central-1",
     :messageId
                        "ade3ae2b-5eb4-47f9-853c-9752833f4a6a",
     :eventSource       "aws:sqs",
     :messageAttributes {},
     :body
                        (util/to-json {:apply
                                                       {:service      "glms-booking-company-svc",
                                                        :aggregate-id agg-id}
                                       :request-id     req-id
                                       :interaction-id int-id})
     :receiptHandle
                        "AQEBc5I/dbrIlclA3F997eED1MfH4LSqv2+jxYoLyESY+luxP1SSM790CGjdl85LGgM8iner9GLA/hHbY2lLruiDq1c+YgbZzD8Kujp+B1/XafH31UFbv+PMzL0EHHYUTEJ0HyIzhV3LceJaAxn9LetZCYrw2+WJjjABx/Y9dJwXnBst6ArSfuqCFBspOtKhwvoW/STEhT07XNc1CKN8S0I2l1sjV8UnRr9+kqLNs5NT2+4daEiKUSe8yeN5hrUHSu803JAySUjDbhSuc9KOMuNYedCsI5e4NnmPKXGqT48tfWm8bD6Rj8/UqIRnxx2pf81rxzQ52566sM1XZwN1Gui4qThajSYoUgcAM4C5Ue+b3FRDmpLSGvEGK4TBLBjwTq8tpUSRCOpTQeiBdQ14ypDLU9XQCSOjLRXIkubjFo1pkddV8KMU2knpbBjnq2MxmZ+S",
     :attributes
                        {:ApproximateReceiveCount "1",
                         :SentTimestamp           "1580103331238",
                         :SenderId                "AIDAISDDSWNBEXIA6J64K",
                         :ApproximateFirstReceiveTimestamp
                                                  "1580103331242"}}]})


(def ctx
  (-> {}
      (assoc :service-name "local-test")
      (event-store/register)
      (view-store/register)
      (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                            {:id       (:id cmd)
                             :event-id :event-1
                             :name     (:name cmd)}))
      (edd/reg-event :event-1
                     (fn [agg event]
                       (merge agg
                              {:value "1"})))
      (edd/reg-event :event-2
                     (fn [agg event]
                       (merge agg
                              {:value "2"})))))

(deftest apply-when-two-events
  (mock/with-mock-dal
    {:event-store [{:event-id  :event-1
                    :event-seq 1
                    :id        agg-id}
                   {:event-id  :event-1
                    :event-seq 2
                    :id        agg-id}]}
    (let [resp (edd/handler ctx req)]
      (mock/verify-state :aggregate-store
                         [{:id      agg-id
                           :version 2
                           :value   "1"}])
      (is (= {:result         {:apply true}
              :interaction-id int-id
              :request-id     req-id}
             resp)))))




(deftest apply-when-no-events
  (mock/with-mock-dal
    (let [resp (event/handle-event
                (merge ctx
                       {:apply          {:aggregate-id
                                         #uuid "cb245f3b-a791-4637-919f-c0682d4277ae",
                                         :service "glms-plc2-svc"},
                        :request-id     #uuid "01ce9e4b-3922-4d93-a95d-a326b4c49b5c",
                        :interaction-id #uuid "01ce9e4b-3922-4d93-a95d-a326b4c49b5c"}))]
      (is (= {:error :no-events-found}
             resp)))))

(deftest apply-when-events-but-no-handler
  (let [ctx
        (-> {}
            (assoc :service-name "local-test")
            (event-store/register)
            (view-store/register)
            (edd/reg-cmd :cmd-1 (fn [ctx cmd]
                                  {:id       (:id cmd)
                                   :event-id :event-1
                                   :name     (:name cmd)})))]

    (mock/with-mock-dal
      {:event-store [{:event-id  :event-1
                      :event-seq 1
                      :id        agg-id}
                     {:event-id  :event-1
                      :event-seq 2
                      :id        agg-id}]}
      (let [resp (edd/handler ctx req)]
        (mock/verify-state :aggregate-store
                           [])
        (is (= {:result {:apply true}
                :interaction-id int-id
                :request-id     req-id}
               resp))))
    ))
