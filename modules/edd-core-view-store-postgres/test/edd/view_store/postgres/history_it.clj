(ns edd.view-store.postgres.history-it
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as event-store]
            [edd.view-store.postgres.view-store :as view-store]
            [edd.core :as edd]
            [edd.s3.view-store :as s3.vs]
            [lambda.uuid :as uuid]
            [lambda.util :as util]
            [edd.common :as common]))

(defn clean-ctx
  []
  (-> {}
      (assoc :service-name "local-test")
      (assoc :response-cache :default)
      (assoc :environment-name-lower (util/get-env "EnvironmentNameLower"))
      (event-store/register)
      (view-store/register)))

(defn get-ctx
  ([] (-> (clean-ctx)
          (edd/reg-query :get-by-id common/get-by-id)
          (edd/reg-query :get-by-id-and-version-v2 common/get-by-id-and-version-v2)
          (edd/reg-cmd :cmd-1 (fn [_ cmd]
                                [{:identity (:id cmd)}
                                 {:sequence (:id cmd)}
                                 {:id       (:id cmd)
                                  :event-id :event-1
                                  :name     (:name cmd)}])
                       :consumes [:map
                                  [:id uuid?]])
          (edd/reg-cmd :cmd-2 (fn [_ cmd]
                                [{:id       (:id cmd)
                                  :event-id :event-2
                                  :name     (:name cmd)}])
                       :consumes [:map
                                  [:id uuid?]])
          (edd/reg-event :event-1
                         (fn [agg _]
                           (merge agg
                                  {:value "1"})))
          (edd/reg-event :event-2
                         (fn [agg _]
                           (merge agg
                                  {:value "2"})))))
  ([invocation-id] (-> (get-ctx)
                       (assoc :invocation-id invocation-id))))

(defn -cmd-and-apply
  [ctx cmd aggregate-id]
  (let [invocation-id
        (uuid/gen)

        request-id
        (uuid/gen)

        interaction-id
        (uuid/gen)

        ctx
        (assoc ctx :invocation-id invocation-id)

        cmd
        (assoc cmd
               :request-id request-id
               :interaction-id interaction-id)

        apply
        {:request-id request-id
         :interaction-id interaction-id
         :meta {:realm :test}
         :apply {:aggregate-id aggregate-id}}]

    (edd/handler ctx cmd)
    (edd/handler ctx apply)))

;; This test is currently executed manually due to complexity of migrations
;; to setup the test `repl/local.sh` script must be executed in order to
;; spin postgres db with all migrations

#_(deftest when-no-historisation-then-no-aggregates-returned
    (with-redefs [s3.vs/store-to-s3 (fn [_ctx] nil)]
      (let [agg-id
            (uuid/gen)

            interaction-id
            (uuid/gen)

            ctx
            (get-ctx)

            ctx
            (assoc-in ctx [:service-configuration :history] :disabled)

            cmds
            {:request-id (uuid/gen)
             :meta {:realm :test}
             :commands [{:cmd-id :cmd-1 :id agg-id}
                        {:cmd-id :cmd-2 :id agg-id}]}]

        (-cmd-and-apply ctx cmds agg-id)

        (let [{aggregate :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id}})
              {aggregate-v2 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 2}})

              {aggregate-v3 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 3}})

              {aggregate-v1 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 1}})]

          (is (= {:value "2",
                  :version 2,
                  :id agg-id}
                 aggregate))
          (is (nil? aggregate-v1))
          (is (nil? aggregate-v2))
          (is (nil? aggregate-v3))))))

#_(deftest when-query-for-aggregate-by-id-and-version
    (with-redefs [s3.vs/store-to-s3 (fn [_ctx] nil)]
      (let [agg-id
            (uuid/gen)

            interaction-id
            (uuid/gen)

            ctx
            (get-ctx)

            cmds
            {:request-id (uuid/gen)
             :meta {:realm :test}
             :commands [{:cmd-id :cmd-1 :id agg-id}
                        {:cmd-id :cmd-2 :id agg-id}]}]

        (-cmd-and-apply ctx cmds agg-id)

        (let [{aggregate :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id}})
              {aggregate-v2 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 2}})

              {aggregate-v3 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 3}})

              {aggregate-v1 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 1}})]

          (is (= {:value "2",
                  :version 2,
                  :id agg-id}
                 aggregate))
          (is (= {:value "1",
                  :version 1,
                  :id agg-id}
                 aggregate-v1))
          (is (= {:value "2",
                  :version 2,
                  :id agg-id}
                 aggregate-v2))
          (is (= nil aggregate-v3))))))

#_(deftest when-two-commands-in-same-tx
    (with-redefs [s3.vs/store-to-s3 (fn [_ctx] nil)]
      (let [agg-id
            (uuid/gen)

            interaction-id
            (uuid/gen)

            ctx
            (get-ctx)

            cmd-1
            {:request-id (uuid/gen)
             :meta {:realm :test}
             :commands [{:cmd-id :cmd-1
                         :id agg-id}]}

            cmd-2
            {:request-id (uuid/gen)
             :meta {:realm :test}
             :commands [{:cmd-id :cmd-2
                         :id agg-id}]}]

      ;; execute two commands to bump aggregate version to 2
        (-cmd-and-apply ctx cmd-1 agg-id)
        (-cmd-and-apply ctx cmd-2 agg-id)

        (let [{aggregate :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id}})
              {aggregate-v2 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 2}})

              {aggregate-v3 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 3}})

              {aggregate-v1 :result}
              (edd/handler ctx
                           {:request-id (uuid/gen)
                            :interaction-id interaction-id
                            :meta {:realm :test}
                            :query {:query-id :get-by-id
                                    :id agg-id
                                    :version 1}})]

          (is (= {:value "2",
                  :version 2,
                  :id agg-id}
                 aggregate))
          (is (= {:value "1",
                  :version 1,
                  :id agg-id}
                 aggregate-v1))
          (is (= {:value "2",
                  :version 2,
                  :id agg-id}
                 aggregate-v2))
          (is (= nil aggregate-v3))))))
