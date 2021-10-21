(ns doc.edd.command-test
  (:require [clojure.test :refer :all]
            [doc.edd.ctx :refer [init-ctx]]
            [edd.core :as edd]
            [edd.test.fixture.dal :as mock]
            [edd.el.cmd :as el-cmd]
            [lambda.uuid :as uuid])
  (:import (clojure.lang ExceptionInfo)))

(deftest command-registration
  (testing "# To register query you need invoke edd.core/reg-cmd function"
    (-> init-ctx
        (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                 (comment "Here you would put handler for command"))))

    (testing "Command requires handler. If handler is nil then exception is raised"
      (is (thrown? ExceptionInfo
                   (-> init-ctx
                       (edd/reg-cmd :test-cmd nil)))))
    (testing "It is poss register command schema using :consumes key"
      (-> init-ctx
          (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                   (comment "Here you would put handler for command"))
                       :consumes [:map
                                  [:first-name :string]])))
    (testing "Schema can also be registered using :spec key but this method is deprecated"
      (-> init-ctx
          (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                   (comment "Here you would put handler for command"))
                       :spec [:map
                              [:first-name :string]])))))

(deftest command-handler
  (testing (str "Command handler is envisioned to be pure function returning one or more events,
            Input is context and command that needs to be processed. Context wil contain
            all needed dependencies for this command (Moore later on dependencies).
            Command handler is mutating data in the system. It is necessary to have
            implementation for backend services. Ww have in memory implementation in
            " (require 'edd.test.fixture.dal) " namespace. We will start with context that is defined
            in this namespace.")
    (let [event {:event-id :tested-cmd}
          ctx (-> mock/ctx
                  (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                           event)))
          cmd-id (uuid/gen)
          request {:cmd-id :test-cmd
                   :id     cmd-id}]

      (comment "To get response for command ve call edd.el.cmd/handle-commands
                This function you normally don't invoke directly. It is used sometimes
                for testing. Response is summarized and only shows number of items")
      (is (= {:effects    []
              :events     1
              :identities 0
              :meta       [{:test-cmd {:id cmd-id}}]
              :sequences  0
              :success    true}
             (el-cmd/handle-commands
              ctx
              {:commands [request]})))

      (comment "To see non summarized response you can pass in context
                :no-summary before invoking handle-commands. this will return
                all data that is output of request. Other fields that are here
                will be analyzed separately.")
      (is (= {:effects    []
              :events     [{:event-id       :tested-cmd
                            :id             cmd-id
                            :event-seq      2
                            :interaction-id nil
                            :meta           {}
                            :request-id     nil}]
              :identities []
              :meta       [{:test-cmd {:id cmd-id}}]
              :sequences  []}
             (el-cmd/handle-commands
              (assoc ctx :no-summary true)
              {:commands [request]}))))
    (testing "Same request can contain multiple commands in vector. And output
              of command can be vector of events"
      (let [event-1 {:event-id :event-1}
            event-2-1 {:event-id :event-2-1}
            event-2-2 {:event-id :event-2-2}
            ctx (-> mock/ctx
                    (edd/reg-cmd :test-cmd-1 (fn [ctx cmd]
                                               event-1))
                    (edd/reg-cmd :test-cmd-2 (fn [ctx cmd]
                                               [event-2-1 event-2-2])))
            cmd-id-1 (uuid/gen)
            cmd-id-2 (uuid/gen)]

        (comment "To get response for command ve call edd.el.cmd/handle-commands
                This function you normally don't invoke directly. It is used sometimes
                for testing. Response is summarized and only shows number of items")
        (is (= {:effects    []
                :events     [{:event-id       :event-1
                              :id             cmd-id-1
                              :event-seq      1
                              :interaction-id nil
                              :meta           {}
                              :request-id     nil}
                             {:event-id       :event-2-1
                              :id             cmd-id-2
                              :event-seq      1
                              :interaction-id nil
                              :meta           {}
                              :request-id     nil}
                             {:event-id       :event-2-2
                              :id             cmd-id-2
                              :event-seq      2
                              :interaction-id nil
                              :meta           {}
                              :request-id     nil}]
                :identities []
                :meta       [{:test-cmd-1 {:id cmd-id-1}}
                             {:test-cmd-2 {:id cmd-id-2}}]
                :sequences  []}
               (el-cmd/handle-commands
                (assoc ctx :no-summary true)
                {:commands [{:id     cmd-id-1
                             :cmd-id :test-cmd-1}
                            {:id     cmd-id-2
                             :cmd-id :test-cmd-2}]})))))))

(deftest effects
  (testing "Outpu"
    (let [event {:event-id :tested-cmd}
          ctx (-> mock/ctx
                  (edd/reg-cmd :test-cmd (fn [ctx cmd]
                                           event)))
          cmd-id (uuid/gen)
          request {:cmd-id :test-cmd
                   :id     cmd-id}]

      (comment "To get response for command ve call edd.el.cmd/handle-commands
                This function you normally don't invoke directly. It is used sometimes
                for testing. Response is summarized and only shows number of items")
      (is (= {:effects    []
              :events     1
              :identities 0
              :meta       [{:test-cmd {:id cmd-id}}]
              :sequences  0
              :success    true}
             (el-cmd/handle-commands
              ctx
              {:commands [request]})))

      (comment "To see non summarized response you can pass in context
                :no-summary before invoking handle-commands. this will return
                all data that is output of request. Other fields that are here
                will be analyzed separately.")
      (is (= {:effects    []
              :events     [{:event-id       :tested-cmd
                            :id             cmd-id
                            :event-seq      2
                            :interaction-id nil
                            :meta           {}
                            :request-id     nil}]
              :identities []
              :meta       [{:test-cmd {:id cmd-id}}]
              :sequences  []}
             (el-cmd/handle-commands
              (assoc ctx :no-summary true)
              {:commands [request]}))))))