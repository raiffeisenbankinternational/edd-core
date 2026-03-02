(ns edd.cmd-spec-test
  (:require [clojure.tools.logging :as log]
            [edd.core :as edd]
            [clojure.test :refer :all]
            [edd.test.fixture.dal :as mock]
            [lambda.uuid :as uuid]
            [edd.el.cmd :as cmd])
  (:import (clojure.lang ExceptionInfo)))

(defn dummy-command-handler
  [ctx cmd]
  (log/info "Dummy" cmd)
  {:event-id :dummy-event
   :id       (:id cmd)
   :handled  true})

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))

(def ctx
  (-> mock/ctx
      (edd/reg-cmd :dummy-cmd dummy-command-handler)))

(def valid-command-request
  {:commands [{:cmd-id :dummy-cmd
               :id     cmd-id}]})

(deftest test-valid-command
  (mock/with-mock-dal
    (cmd/handle-commands ctx valid-command-request)
    (mock/verify-state :event-store [{:event-id  :dummy-event
                                      :handled   true
                                      :event-seq 1
                                      :id        cmd-id}])
    (mock/verify-state :identities [])
    (mock/verify-state :commands [])))

(deftest test-missing-id-command
  (mock/with-mock-dal
    (is (= {:error {:id ["missing required key"]}}
           (select-keys
            (mock/handle-cmd
             ctx
             {:cmd-id :dummy-cmd})
            [:error])))))

(deftest test-missing-failed-custom-validation-command
  (mock/with-mock-dal
    (is (= {:name ["missing required key"]}
           (:error
            (mock/handle-cmd
             (-> ctx
                 (edd/reg-cmd :dummy-cmd dummy-command-handler
                              :consumes [:map
                                         [:name string?]]))
             {:cmd-id :dummy-cmd
              :id     cmd-id}))))))

(deftest test-missing-failed-custom-validation-command-logs-error
  (mock/with-mock-dal
    (let [error-log
          (atom nil)

          error-count
          (atom 0)

          result
          (with-redefs [log/log* (fn [_ level _ message]
                                   (when (= :error level)
                                     (swap! error-count inc)
                                     (reset! error-log message)))]
            (mock/handle-cmd
             (-> ctx
                 (edd/reg-cmd :dummy-cmd dummy-command-handler
                              :consumes [:map
                                         [:name string?]]))
             {:cmd-id :dummy-cmd
              :id     cmd-id}))]
      (is
       (= {:error {:name ["missing required key"]}}
          (select-keys result [:error])))

      (is
       (= 1 @error-count))

      (is
       (re-matches #".*Command validation failed.*:dummy-cmd.*missing required key.*"
                   @error-log)))))

(deftest test-reserved-key-user-in-event
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id :bad-event
                       :user "should-not-be-here"}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for reserved key :user")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-reserved-key-role-in-event
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id :bad-event
                       :role :admin}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for reserved key :role")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-reserved-key-meta-in-event
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id :bad-event
                       :meta {:something "sneaky"}}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for reserved key :meta")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-reserved-key-request-id-in-event
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id :bad-event
                       :request-id (uuid/gen)}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for reserved key :request-id")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-reserved-key-event-seq-in-event
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id :bad-event
                       :event-seq 42}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for reserved key :event-seq")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-multiple-reserved-keys-in-event
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id :bad-event
                       :user "bad"
                       :role :bad
                       :meta {:bad true}}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for multiple reserved keys")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-event-id-must-be-keyword
  (mock/with-mock-dal
    (try
      (mock/handle-cmd
       (edd/reg-cmd mock/ctx
                    :bad-cmd
                    (fn [_ cmd]
                      {:event-id "not-a-keyword"}))
       {:cmd-id :bad-cmd
        :id cmd-id})
      (is false
          "Expected exception for non-keyword :event-id")
      (catch ExceptionInfo e
        (is
         (some?
          (:error (ex-data e))))))))

(deftest test-valid-event-without-reserved-keys
  (mock/with-mock-dal
    (let [result
          (mock/handle-cmd
           (edd/reg-cmd mock/ctx
                        :good-cmd
                        (fn [_ cmd]
                          {:event-id :good-event
                           :attrs {:data "valid"}}))
           {:cmd-id :good-cmd
            :id cmd-id})]

      (is
       (true?
        (:success result))))))
