(ns edd.meta-annotations-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [edd.core :as edd]
            [edd.test.fixture.dal :as mock]
            [lambda.util :as util]))

(def fixed-created-on "2026-05-01T00:00:00.000+00:00")

(use-fixtures :each
  (fn [t]
    (with-redefs [util/date->string (fn ([] fixed-created-on)
                                      ([_] fixed-created-on))]
      (t))))

(def request-id #uuid "11111111-1111-1111-1111-111111111111")
(def interaction-id #uuid "22222222-2222-2222-2222-222222222222")
(def user-id "user-1@example.com")
(def aggregate-id #uuid "33333333-3333-3333-3333-333333333333")

(defn cmd-handler [_ctx cmd]
  {:event-id :thing-created
   :id (:id cmd)
   :name (:name cmd)})

(defn evt-handler [agg event]
  (assoc agg :name (:name event)))

(defn ctx-with-user
  ([]
   (ctx-with-user user-id))
  ([uid]
   (-> mock/ctx
       (edd/reg-cmd :create-thing cmd-handler)
       (edd/reg-cmd :update-thing
                    (fn [_ctx cmd]
                      {:event-id :thing-updated
                       :id (:id cmd)
                       :name (:name cmd)}))
       (edd/reg-event :thing-created evt-handler)
       (edd/reg-event :thing-updated evt-handler)
       (assoc :user {:id uid :role :user :email uid})
       (assoc-in [:meta :user]
                 {:id uid :role :user :email uid}))))

(defn first-aggregate
  []
  (first (mock/peek-state :aggregate-store)))

(deftest events-carry-created-on-in-meta
  (testing "Stored events have :meta :created-on populated"
    (mock/with-mock-dal
      {:keep-meta true}
      (let [ctx
            (ctx-with-user)]

        (mock/handle-cmd
         (assoc ctx
                :request-id request-id
                :interaction-id interaction-id)
         {:cmd-id :create-thing
          :id aggregate-id
          :name "Foo"
          :request-id request-id
          :interaction-id interaction-id})

        (let [event
              (first (mock/peek-state :event-store))]

          (is
           (=
            fixed-created-on
            (get-in event [:meta :created-on])))

          (is
           (=
            request-id
            (:request-id event)))

          (is
           (=
            interaction-id
            (:interaction-id event)))

          (is
           (=
            user-id
            (:user event))))))))

(deftest aggregate-gets-annotations-on-first-write
  (testing "First write fills :meta :annotations with both created-* and updated-*"
    (mock/with-mock-dal
      {:keep-meta true}
      (let [ctx
            (ctx-with-user)]

        (mock/apply-cmd
         (assoc ctx
                :request-id request-id
                :interaction-id interaction-id)
         {:cmd-id :create-thing
          :id aggregate-id
          :name "Foo"
          :request-id request-id
          :interaction-id interaction-id})

        (is
         (=
          {:created-on         fixed-created-on
           :updated-on         fixed-created-on
           :created-request-id request-id
           :updated-request-id request-id
           :invocation-id      nil
           :interaction-id     interaction-id
           :created-user-id    user-id
           :updated-user-id    user-id}
          (get-in (first-aggregate) [:meta :annotations])))))))

(deftest aggregate-preserves-created-on-update
  (testing "Subsequent writes preserve created-* fields and refresh updated-*"
    (mock/with-mock-dal
      {:keep-meta true}
      (let [req-1
            #uuid "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

            req-2
            #uuid "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

            user-1
            "first@example.com"

            user-2
            "second@example.com"

            ctx-1
            (-> (ctx-with-user user-1)
                (assoc :request-id req-1
                       :interaction-id interaction-id))

            ctx-2
            (-> (ctx-with-user user-2)
                (assoc :request-id req-2
                       :interaction-id interaction-id))]

        (mock/apply-cmd
         ctx-1
         {:cmd-id :create-thing
          :id aggregate-id
          :name "Original"
          :request-id req-1
          :interaction-id interaction-id})

        (mock/apply-cmd
         ctx-2
         {:cmd-id :update-thing
          :id aggregate-id
          :name "Updated"
          :request-id req-2
          :interaction-id interaction-id})

        (let [annotations
              (get-in (first-aggregate) [:meta :annotations])]

          (is
           (=
            user-1
            (:created-user-id annotations)))

          (is
           (=
            user-2
            (:updated-user-id annotations)))

          (is
           (=
            req-1
            (:created-request-id annotations)))

          (is
           (=
            req-2
            (:updated-request-id annotations))))))))
