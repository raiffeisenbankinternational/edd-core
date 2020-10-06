(ns edd.cmd-spec-test
  (:require [clojure.tools.logging :as log]
            [edd.core :as edd]
            [edd.dal :as dal]
            [edd.el.cmd :as cmd]
            [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [lambda.uuid :as uuid]))

(defn dummy-command-handler
  [ctx cmd]
  (log/info "Dummy" cmd)
  {:event-id :dummy-event
   :id       (:id cmd)
   :handled  true})

(def cmd-id (uuid/parse "111111-1111-1111-1111-111111111111"))

(defn execute-command
  [ctx cmd]
  "Test if id-fn works correctly together with event seq. This test does multiple things. Sorry!!"
  (with-redefs [dal/store-event (fn [ctx realm events] events)
                dal/store-identity (fn [ctx idt] idt)
                dal/store-sequence (fn [ctx sequence] sequence)
                dal/store-cmd (fn [ctx cmd] cmd)
                dal/read-realm (fn [ctx])
                cmd/resolve-local-dependency (fn [ctx cmd req]
                                               (log/info "Mocking local dependency")
                                               {:id cmd-id})
                dal/get-max-event-seq (fn [ctx id]
                                        (get {cmd-id 21}
                                             id))
                dal/with-transaction (fn [ctx fn] (fn ctx))]
    (cmd/get-commands-response
     ctx
     cmd)))

(defn prepare-no-spec
  [ctx]
  (-> ctx
      (edd/reg-cmd :dummy-cmd dummy-command-handler)))

(def valid-command-request
  {:commands [{:cmd-id :dummy-cmd
               :id     cmd-id}]})

(deftest test-valid-command
  (with-redefs [dal/log-dps (fn [ctx] ctx)]
    (let [resp (execute-command
                (prepare-no-spec {})
                valid-command-request)]
      (is (= {:events     [{:event-id  :dummy-event
                            :handled   true
                            :event-seq 22
                            :id        cmd-id}],
              :identities [],
              :sequences  [],
              :commands   []}
             (dissoc resp :meta))))))

(def command-request-missing-id
  {:commands [{:cmd-id :dummy-cmd}]})

(deftest test-missing-id-command
  (let [resp (execute-command
              (prepare-no-spec {})
              command-request-missing-id)]
    (is (= {:error '({:id ["missing required key"]})}
           resp))))

(def command-request-custom-missing
  {:commands [{:cmd-id :dummy-cmd
               :id     cmd-id}]})

(defn prepare-with-spec
  [ctx]
  (-> ctx
      (edd/reg-cmd :dummy-cmd dummy-command-handler
                   :spec [:map
                          [:name string?]])))

(deftest test-missing-failed-custom-validation-command
  (let [resp (execute-command
              (prepare-with-spec {})
              command-request-custom-missing)]
    (is (= {:error '({:name ["missing required key"]})}
           resp))))
