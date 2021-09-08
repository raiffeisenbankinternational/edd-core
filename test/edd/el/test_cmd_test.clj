(ns edd.el.test-cmd-test
  (:require [clojure.test :refer :all]
            [edd.el.cmd :as cmd]
            [edd.el.query :as query]
            [lambda.util :as util]
            [lambda.uuid :as uuid]
            [edd.dal :as dal]
            [edd.memory.event-store :as event-store]
            [edd.response.s3 :as s3-cache])
  (:import (clojure.lang ExceptionInfo)))

(def cmd-id (uuid/gen))

(deftest test-prepare-context-no-dependencies
  "Test if context if properly prepared for no-dependencies"
  (with-redefs [query/handle-query (fn [ctx q] {:response true})
                dal/log-dps (fn [ctx] ctx)
                dal/get-max-event-seq (fn [ctx id] 0)]
    (let [ctx (cmd/resolve-dependencies-to-context
               {:commands [{:cmd-id :test-cmd
                            :id cmd-id}]})]
      (is (= {:dps-resolved {}}
             (dissoc ctx :commands))))))

(deftest test-prepare-context-for-command-local
  "Test if context if properly prepared for local queries. If local query return nill
  we do not expect context to change. This will be used for testing on other servies
  to prearrange context"
  (with-redefs [dal/log-dps (fn [ctx] ctx)
                dal/get-max-event-seq (fn [ctx id] 0)]
    (let [ctx {:dps {:test-cmd
                     {:test-value (fn [cmd]
                                    {:query-id :q1
                                     :query {}})
                      :test-value-2 (fn [cmd]
                                      {:query-id :q2
                                       :query {}})}}
               :commands [{:cmd-id :test-cmd
                           :id cmd-id}]}]
      (with-redefs [query/handle-query (fn [ctx q]
                                         (if (= (get-in q [:query :query-id]) :q1)
                                           {:response true}
                                           nil))]
        (let [resolved-ctx (cmd/resolve-dependencies-to-context ctx)]
          (is (= {:dps-resolved {:test-value {:response true}}}
                 (dissoc resolved-ctx
                         :dps
                         :commands)))))
      (testing "If error is returned"
        (with-redefs [query/handle-query (fn [ctx q]
                                           (if (= (get-in q [:query :query-id]) :q1)
                                             {:error true}
                                             nil))]
          (is (thrown? ExceptionInfo
                       (cmd/resolve-dependencies-to-context ctx)))))
      (testing "If resolved returns exception"
        (with-redefs [query/handle-query (fn [ctx q]
                                           (if (= (get-in q [:query :query-id]) :q1)
                                             (throw (ex-info "Some" {:error :happened}))
                                             nil))]
          (is (thrown? ExceptionInfo
                       (cmd/resolve-dependencies-to-context ctx))))))))

(deftest test-prepare-context-for-command-remote-deps
  "Test if context if properly prepared for remote queries"
  (with-redefs [aws/get-token (fn [ctx] "")
                dal/log-dps (fn [ctx] ctx)
                dal/get-max-event-seq (fn [ctx id] 0)]
    (let [ctx {:dps {:test-cmd
                     {:test-value {:service :remote
                                   :query (fn [cmd]
                                            {:query-id :q1
                                             :query {}})}}}
               :commands [{:cmd-id :test-cmd
                           :id cmd-id}]}]
      (with-redefs [util/http-post (fn [ctx q]
                                     {:status 200
                                      :body {:result {:response true}}})]
        (let [resolved-ctx (cmd/resolve-dependencies-to-context ctx)]
          (is (= {:dps-resolved {:test-value {:response true}}}
                 (dissoc resolved-ctx
                         :dps
                         :commands)))))
      (testing "If error is returned"
        (with-redefs [util/http-post (fn [ctx q]
                                       {:error "ConnectionTimeout"})]
          (is (thrown? ExceptionInfo
                       (cmd/resolve-dependencies-to-context ctx)))))
      (testing "If resolved returns exception"
        (with-redefs [util/http-post (fn [ctx q]
                                       {:status 499
                                        :body {:response true}})]
          (is (thrown? ExceptionInfo
                       (cmd/resolve-dependencies-to-context ctx))))))))

(def id-1 (uuid/gen))
(def id-2 (uuid/gen))

(deftest test-resolve-id
  "Test if id is properly resolved"
  (let [id (cmd/resolve-command-id
            {}
            {:cmd-id :dummy-cmd
             :id id-1})]
    (is (= id {:cmd-id :dummy-cmd
               :id id-1}))))

(deftest test-resolve-id-when-id-override-present
  "Test if id is properly resolved when override present"
  (let [id (cmd/resolve-command-id
            {:id-fn {:dummy-cmd (fn [ctx cmd] (+ (:dps-1 ctx)
                                                 (:dps-2 ctx)))}
             :dps-1 2
             :dps-resolved {:dps-2 3}}
            {:cmd-id :dummy-cmd
             :id id-1})]
    (is (= id {:cmd-id :dummy-cmd
               :id 5
               :original-id id-1}))))

(deftest test-resolve-id-when-id-override-returns-nil
  "Test if id is properly resolved when override method return nil, we should fallback to (:id cmd)"
  (let [id (cmd/resolve-command-id
            {:id-fn {:dummy-cmd (fn [ctx cmd] nil)}}
            {:cmd-id :dummy-cmd
             :id id-1})]
    (is (= id {:cmd-id :dummy-cmd
               :id id-1}))))