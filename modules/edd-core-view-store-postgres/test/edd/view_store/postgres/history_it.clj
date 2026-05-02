(ns edd.view-store.postgres.history-it
  (:require [clojure.test :refer :all]
            [edd.postgres.event-store :as event-store]
            [edd.view-store.postgres.view-store :as view-store]
            [edd.core :as edd]
            [edd.s3.view-store :as s3.vs]
            [edd.test.fixture.dates :as dates]
            [lambda.uuid :as uuid]
            [lambda.util :as util]
            [edd.common :as common]))

(use-fixtures :each
  (dates/per-request-fixture)
  (fn [t]
    (with-redefs [s3.vs/store-to-s3 (fn [_ctx _aggregate] nil)
                  s3.vs/get-from-s3 (fn [_ctx _id] nil)]
      (t))))

(defn clean-ctx
  []
  (-> {}
      (assoc :service-name :local-test)
      (assoc :response-cache :default)
      (assoc :hosted-zone-name (util/get-env "PublicHostedZoneName" "example.com"))
      (assoc :environment-name-lower (util/get-env "EnvironmentNameLower"))
      (event-store/register)
      (view-store/register)))

(defn get-ctx
  ([] (-> (clean-ctx)
          (edd/reg-query :get-by-id common/get-by-id)
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

(defn get-ctx-2
  ([] (-> (clean-ctx)
          (edd/reg-query :get-by-id common/get-by-id)
          (edd/reg-cmd :cmd-1 (fn [_ cmd]
                                [{:identity (:id cmd)}
                                 {:sequence (:id cmd)}
                                 {:id       (:id cmd)
                                  :event-id :event-1
                                  :name     (:name cmd)}
                                 {:id       (:id cmd)
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

    (binding [dates/*current-request-id* request-id]
      (edd/handler ctx cmd)
      (edd/handler ctx apply))

    {:request-id request-id
     :interaction-id interaction-id
     :invocation-id invocation-id}))

(defn -annotations
  [{:keys [request-id interaction-id invocation-id]} created-on updated-on]
  {:created-on         created-on
   :updated-on         updated-on
   :created-request-id request-id
   :updated-request-id request-id
   :created-user-id    nil
   :updated-user-id    nil
   :interaction-id     interaction-id
   :invocation-id      invocation-id})

(defn -annotations-2
  "Annotations for an aggregate written across two separate requests:
   created-* preserved from `create-ids`, updated-* and interaction/invocation
   taken from the last write `update-ids`."
  [create-ids update-ids created-on updated-on]
  {:created-on         created-on
   :updated-on         updated-on
   :created-request-id (:request-id create-ids)
   :updated-request-id (:request-id update-ids)
   :created-user-id    nil
   :updated-user-id    nil
   :interaction-id     (:interaction-id update-ids)
   :invocation-id      (:invocation-id update-ids)})

;; This test is currently executed manually due to complexity of migrations
;; to setup the test `repl/local.sh` script must be executed in order to
;; spin postgres db with all migrations
(deftest when-no-historisation-then-no-aggregates-returned
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
                    {:cmd-id :cmd-2 :id agg-id}]}

        ids
        (-cmd-and-apply ctx cmds agg-id)

        [d1 d2]
        (dates/dates-for (:request-id ids))]

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

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations ids d1 d2)}}
        aggregate))

      (is
       (nil? aggregate-v1))

      (is
       (nil? aggregate-v2))

      (is
       (nil? aggregate-v3)))))

(deftest when-query-for-aggregate-by-id-and-version
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
                    {:cmd-id :cmd-2 :id agg-id}]}

        ids
        (-cmd-and-apply ctx cmds agg-id)

        [d1 d2]
        (dates/dates-for (:request-id ids))]

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

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations ids d1 d2)}}
        aggregate))

      (is
       (=
        {:value "1"
         :version 1
         :id agg-id
         :meta {:annotations (-annotations ids d1 d1)}}
        aggregate-v1))

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations ids d1 d2)}}
        aggregate-v2))

      (is
       (=
        nil
        aggregate-v3)))))

(deftest when-two-commands-in-different-tx
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
                     :id agg-id}]}

        ids-1
        (-cmd-and-apply ctx cmd-1 agg-id)

        ids-2
        (-cmd-and-apply ctx cmd-2 agg-id)

        [d1]
        (dates/dates-for (:request-id ids-1))

        [d2]
        (dates/dates-for (:request-id ids-2))]

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

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations-2 ids-1 ids-2 d1 d2)}}
        aggregate))

      (is
       (=
        {:value "1"
         :version 1
         :id agg-id
         :meta {:annotations (-annotations ids-1 d1 d1)}}
        aggregate-v1))

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations-2 ids-1 ids-2 d1 d2)}}
        aggregate-v2))

      (is
       (=
        nil
        aggregate-v3)))))

(deftest when-multiple-events-then-only-one-history-entry
  (let [agg-id
        (uuid/gen)

        interaction-id
        (uuid/gen)

        ctx
        (get-ctx-2)

        cmds
        {:request-id (uuid/gen)
         :meta {:realm :test}
         :commands [{:cmd-id :cmd-1 :id agg-id}]}

        ids
        (-cmd-and-apply ctx cmds agg-id)

        [d1]
        (dates/dates-for (:request-id ids))]

    (let [{aggregate :result}
          (edd/handler ctx
                       {:request-id (uuid/gen)
                        :interaction-id interaction-id
                        :meta {:realm :test}
                        :query {:query-id :get-by-id
                                :id agg-id}})

          {aggregate-v1 :result}
          (edd/handler ctx
                       {:request-id (uuid/gen)
                        :interaction-id interaction-id
                        :meta {:realm :test}
                        :query {:query-id :get-by-id
                                :id agg-id
                                :version 1}})

          {aggregate-v2 :result}
          (edd/handler ctx
                       {:request-id (uuid/gen)
                        :interaction-id interaction-id
                        :meta {:realm :test}
                        :query {:query-id :get-by-id
                                :id agg-id
                                :version 2}})]

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations ids d1 d1)}}
        aggregate))

      (is
       (nil? aggregate-v1))

      (is
       (=
        {:value "2"
         :version 2
         :id agg-id
         :meta {:annotations (-annotations ids d1 d1)}}
        aggregate-v2)))))

(deftest when-two-commands-two-aggregates-then-two-versions
  (let [agg-id
        (uuid/gen)

        agg-2-id
        (uuid/gen)

        interaction-id
        (uuid/gen)

        ctx
        (get-ctx)

        cmds
        {:request-id (uuid/gen)
         :meta {:realm :test}
         :commands [{:cmd-id :cmd-1 :id agg-id}
                    {:cmd-id :cmd-1 :id agg-2-id}]}

        ids
        (-cmd-and-apply ctx cmds agg-id)

        [d1 d2]
        (dates/dates-for (:request-id ids))]

    (let [{aggregate-1-v1 :result}
          (edd/handler ctx
                       {:request-id (uuid/gen)
                        :interaction-id interaction-id
                        :meta {:realm :test}
                        :query {:query-id :get-by-id
                                :id agg-id
                                :version 1}})

          {aggregate-2-v1 :result}
          (edd/handler ctx
                       {:request-id (uuid/gen)
                        :interaction-id interaction-id
                        :meta {:realm :test}
                        :query {:query-id :get-by-id
                                :id agg-2-id
                                :version 1}})]

      (is
       (=
        {:value "1"
         :version 1
         :id agg-id
         :meta {:annotations (-annotations ids d1 d1)}}
        aggregate-1-v1))

      (is
       (=
        {:value "1"
         :version 1
         :id agg-2-id
         :meta {:annotations (-annotations ids d2 d2)}}
        aggregate-2-v1)))))
