(ns ^:docker edd.view-store.postgres.api-test-docker
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is use-fixtures testing]]
   [edd.search :as search]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.fixtures :as fix
    :refer
    [*DB*
     with-conn
     fix-with-db
     fix-with-db-init
     fix-truncate-db
     fix-import-entity]]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.import :as import]
   [edd.view-store.postgres.svc.dimension :as dimension]
   [edd.view-store.postgres.view-store]
   [lambda.util :as util]
   [lambda.uuid :as uuid]
   [next.jdbc :as jdbc])
  (:import
   java.io.File))

(use-fixtures :once
  fix-with-db
  fix-with-db-init)

(use-fixtures :each
  fix-truncate-db
  (fix-import-entity :facility))

(defn ->ctx
  ([]
   (->ctx nil))
  ([overrides]
   (merge {:con (delay *DB*)
           :view-store :postgres
           :service-name c/SVC_TEST
           :meta {:realm c/TEST_REALM}}
          overrides)))

(deftest test-get-by-id-ok

  (let [uuid
        #uuid "3b5e4bae-5a66-4f61-ae37-41f9b5c55873"

        facility
        (api/get-by-id *DB*
                       c/TEST_REALM
                       c/SVC_TEST
                       uuid)]

    (is (= "FAC-8*2711*RLRS"
           (-> facility :attrs :external-id)))))

(deftest test-upsert-single

  (let [uuid
        #uuid "3b5e4bae-5a66-4f61-ae37-41f9b5c55873"

        facility-old
        (api/get-by-id *DB* c/TEST_REALM c/SVC_TEST uuid)

        facility-to-upsert
        {:id uuid
         :attrs {:external-id "123-456"
                 :foo 42}}

        result
        (api/upsert *DB* c/TEST_REALM c/SVC_TEST facility-to-upsert)

        facility-new
        (api/get-by-id *DB* c/TEST_REALM c/SVC_TEST uuid)]

    (is (= (:id facility-old)
           (:id facility-new)))

    (is (= uuid (:id result)))

    (is (= "FAC-8*2711*RLRS"
           (-> facility-old :attrs :external-id)))

    (is (= "123-456"
           (-> facility-new :attrs :external-id)))))

(deftest test-underscores-and-hyphens
  (let [result
        (jdbc/execute! *DB* ["select 1 as number_one, 2 as \"number/two\""])]
    (is (= [{:number_one 1 :number/two 2}]
           result))))

(deftest test-upsert-multiple

  (let [uuid1
        #uuid "3b5e4bae-5a66-4f61-ae37-41f9b5c55873"

        uuid2
        #uuid "6e53d1f2-87d0-4964-b945-fc130e0a3da1"

        facilities-old
        (api/get-by-ids *DB* c/TEST_REALM c/SVC_TEST [uuid1 uuid2])

        f1
        {:id uuid1
         :attrs {:external-id "123"}}

        f2
        {:id uuid2
         :attrs {:external-id "456"}}

        result
        (api/upsert-many *DB* c/TEST_REALM c/SVC_TEST [f1 f2])

        facilities-new
        (api/get-by-ids *DB* c/TEST_REALM c/SVC_TEST [uuid1 uuid2])

        facilities-new
        (->> facilities-new (sort-by :id) vec)]

    (is (= 2 (count facilities-old)))

    (is (= "FAC-8*2711*RLRS"
           (-> facilities-old (get 0) :attrs :external-id)))

    (is (= "FAC-9*2911*TLSK"
           (-> facilities-old (get 1) :attrs :external-id)))

    (is (vector? result))

    (is (= "123"
           (-> facilities-new (get 0) :attrs :external-id)))

    (is (= "456"
           (-> facilities-new (get 1) :attrs :external-id)))))

(deftest test-upsert-multiple-empty
  (let [result
        (api/upsert-many *DB* c/TEST_REALM c/SVC_TEST [])]
    (is (nil? result))))

(deftest test-get-by-id-not-found

  (let [uuid
        (uuid/gen)

        facility
        (api/get-by-id *DB* c/TEST_REALM c/SVC_TEST uuid)]

    (is (nil? facility))))

(deftest test-get-by-ids

  (let [uuid1
        #uuid "3b5e4bae-5a66-4f61-ae37-41f9b5c55873"

        uuid2
        #uuid "6e53d1f2-87d0-4964-b945-fc130e0a3da1"

        [facility1
         facility2
         facility3]
        (api/get-by-ids *DB*
                        c/TEST_REALM c/SVC_TEST
                        [uuid1 uuid2])]

    (is (= "FAC-8*2711*RLRS"
           (-> facility1 :attrs :external-id)))

    (is (= "FAC-9*2911*TLSK"
           (-> facility2 :attrs :external-id)))

    (is (nil? facility3))))

(deftest test-find-by-attrs

  (let [attrs
        {:attrs.risk-on-id #uuid "c0000000-0000-0000-0000-000000232206"
         :attrs.risk-taker-id #uuid "b0000000-0000-0000-0000-000082767282"
         :attrs.product-set-id #uuid "f0000000-0000-0000-0000-000076796578"
         :attrs.fat1 "GPF"
         :attrs.fat2 "GPF01"
         :attrs.tenor-structure.end-date "2023-03-31"
         :attrs.lms-id "LMSID-1000022665"
         :attrs.external-id "FAC-3*2322*RLHR"
         :attrs.status [:approved :pending]}

        facilities
        (api/find-by-attrs *DB*
                           c/TEST_REALM c/SVC_TEST
                           attrs
                           {:order-by [[:created_at :desc]]})]

    (is (some? facilities))))

(deftest test-find-filter

  (let [filter
        [:and
         [:eq :attrs.risk-on-id #uuid "c0000000-0000-0000-0000-000000232206"]
         [:eq :attrs.risk-taker-id #uuid "b0000000-0000-0000-0000-000082767282"]
         [:eq :attrs.product-set-id #uuid "f0000000-0000-0000-0000-000076796578"]
         [:eq :attrs.fat1 "GPF"]
         [:eq :attrs.fat2 "GPF01"]
         [:eq :attrs.tenor-structure.end-date "2023-03-31"]
         [:eq :attrs.lms-id "LMSID-1000022665"]
         [:eq :attrs.external-id "FAC-3*2322*RLHR"]
         [:in :attrs.status [:approved :pending]]]

        facilities
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_TEST
                           {:filter filter
                            :from 0
                            :size 42})]

    (is (= 1 (count facilities)))))

(deftest test-find-advanced-select

  (let [filter
        [:eq :id #uuid "9c86240b-64bf-4a7a-a48a-f27208194835"]

        facilities
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_TEST
                           {:filter filter
                            :from 0
                            :select [:id
                                     "version"
                                     "attrs.migration-date"
                                     :attrs.history.event-id
                                     :attrs.tenor-structure.amount.eur
                                     "attrs.pricing"
                                     :fat2
                                     :dunno]
                            :size 42})]

    (is (= {:id #uuid "9c86240b-64bf-4a7a-a48a-f27208194835",
            :version "4"
            :attrs
            {:migration-date "2023-09-12"
             :history
             [{:event-id :facility-accepted}
              {:event-id :facility-accepted}]
             :tenor-structure [{:amount {:eur "85000000"}}]
             :pricing
             {:transfer-pricing-floor false,
              :smart-asset? false,
              :commission-and-fees? false}}}
           (first facilities)))))

(deftest test-dump-aggregates-filter

  (let [uuids
        [#uuid "3b5e4bae-5a66-4f61-ae37-41f9b5c55873"
         #uuid "9c86240b-64bf-4a7a-a48a-f27208194835"]

        filter
        [:in :id uuids]

        file
        (File/createTempFile "test" ".csv")

        result
        (api/dump-aggregates *DB*
                             c/TEST_REALM
                             c/SVC_TEST
                             file
                             {:filter filter})

        rows
        (api/read-aggregates-dump file)]

    (is (= 2 result))

    (is (= (set uuids)
           (set (mapv :id rows))))))

(deftest test-dump-aggregates-no-filter

  (let [uuid-weird
        (uuid/gen)

        _
        (api/upsert *DB* c/TEST_REALM c/SVC_TEST
                    {:id uuid-weird
                     :attrs {:foo "aaa\"bbb\ttess"}})

        file
        (File/createTempFile "test" ".csv")

        result
        (api/dump-aggregates *DB*
                             c/TEST_REALM
                             c/SVC_TEST
                             file)

        attrs
        [:id
         "version"
         "attrs.migration-date"
         :attrs.history.event-id
         :fat1
         :fat2
         :attrs.foo
         :dunno]

        rows
        (api/read-aggregates-dump file attrs)

        row-weird
        (some (fn [row]
                (when (-> row :id (= uuid-weird))
                  row))
              rows)]

    (is (= 2658 result))
    (is (= result (count rows)))

    (is (= {:attrs {:foo "aaa\"bbb\ttess", :history nil}}
           (dissoc row-weird :id)))

    (is (= {:version "3", :attrs {:history nil}}
           (-> rows first (dissoc :id))))))

(deftest test-find-open-search-nested-case

  (testing "nothing is found: each predicate matches its own journal entry"
    (let [query
          [:nested :attrs.journal
           [:and
            [:= :attrs.journal.time "2023-09-14T07:44:54.072Z"]
            [:= :attrs.journal.event-id :facility-approved]]]

          facilities
          (api/find-advanced *DB*
                             c/TEST_REALM c/SVC_TEST
                             {:filter query
                              :from 0
                              :size 5
                              :sort [:attrs.journal.time :desc]})]

      (is (empty? facilities))))

  (testing "found: both predicate match the same journal entry"
    (let [query
          [:nested :attrs.journal
           [:and
            [:= :attrs.journal.time "2023-09-14T07:44:56.814Z"]
            [:= :attrs.journal.event-id :facility-approved]]]

          facilities
          (api/find-advanced *DB*
                             c/TEST_REALM c/SVC_TEST
                             {:filter query
                              :from 0
                              :size 5
                              :sort [:attrs.journal.time :desc]})]

      (is (= 1 (count facilities))))))

(deftest test-nested-find-by-attrs

  (let [uuid
        (uuid/gen)

        facility
        {:id uuid
         :attrs {:foo-bar-baz 42
                 :history
                 [{:event :accepted
                   :users [{:id "test1@test.com"}
                           {:id "test2@test.com"}]}
                  {:event :processed
                   :users [{:id "test3@test.com"}
                           {:id "test4@test.com"}]}
                  {:event :closed
                   :users [{:id "test5@test.com"}
                           {:id "test6@test.com"}]}]}}

        _
        (api/upsert *DB* c/TEST_REALM c/SVC_TEST facility)

        ;; This case demonstrates a flaw of search in nested JSON arrays.
        ;; Namely, the :accepted event belongs to the first entry, and
        ;; the user test6 belongs to the third one. But since both paths
        ;; produce non-empty result, the document is considered being matched.
        ;; See https://opensearch.org/docs/2.13/field-types/supported-field-types/nested/
        ;; This case is solved in find-advanced when using :nested operator.
        ;; ~Ivan

        attrs
        {:attrs.history.event :accepted
         :attrs.history.users.id "test6@test.com"}

        result
        (api/find-by-attrs *DB* c/TEST_REALM c/SVC_TEST attrs)]

    (is (= 1 (count result)))
    (is (= 42 (-> result first :attrs :foo-bar-baz)))))

(deftest test-nested-find-advanced-cases

  (let [uuid
        (uuid/gen)

        facility
        {:id uuid
         :attrs
         {:foo-bar-baz 42
          :history
          [{:event :accepted
            :users [{:id "test1@test.com"}
                    {:id "test2@test.com"}]}
           {:event :processed
            :users [{:id "test3@test.com"}
                    {:id "test4@test.com"}]}
           {:event :closed
            :users [{:id "test5@test.com"}
                    {:id "test6@test.com"}]}]}}

        _
        (api/upsert *DB* c/TEST_REALM c/SVC_TEST facility)

        ;; paths belong to different nested elements
        ;; false positive match
        query-wrong
        {:filter
         [:and
          [:= :attrs.history.event :accepted]
          [:= :attrs.history.users.id "test6@test.com"]]}

        result-wrong
        (api/find-advanced *DB* c/TEST_REALM c/SVC_TEST query-wrong)

        ;; paths belong to different nested elements
        ;; nested attr is set -> not found
        query-ok-miss
        {:filter
         [:nested :attrs.history
          [:and
           [:= :attrs.history.event :accepted]
           [:= :attrs.history.users.id "test6@test.com"]]]}

        result-ok-miss
        (api/find-advanced *DB* c/TEST_REALM c/SVC_TEST query-ok-miss)

        ;; paths belong to the same nested element
        ;; nested attr is set -> found
        query-ok-hit
        {:filter
         [:nested :attrs.history
          [:and
           [:= :attrs.history.event :processed]
           [:eq :attrs.history.users.id "test3@test.com"]]]}

        result-ok-hit
        (api/find-advanced *DB* c/TEST_REALM c/SVC_TEST query-ok-hit)]

    (is (= 1 (count result-wrong)))
    (is (= [] result-ok-miss))
    (is (= [uuid]
           (mapv :id result-ok-hit)))))

(deftest test-advanced-with-wildcard

  (let [uuid1
        (uuid/gen)

        uuid2
        (uuid/gen)

        uuid3
        (uuid/gen)

        facility1
        {:id uuid1
         :attrs
         {:foo
          [{:users [{:id "1test1@acme.com"}
                    {:id "2test2@acme.com"}]}]}}

        facility2
        {:id uuid2
         :attrs
         {:foo
          [{:users [{:id "3test3@acme.com"}
                    {:id "4test4@acme.com"}]}]}}

        facility3
        {:id uuid3
         :attrs
         {:foo
          [{:users [{:id "AAA@acme.com"}
                    {:id "BBB@acme.com"}]}]}}

        _
        (api/upsert-many *DB* c/TEST_REALM c/SVC_TEST [facility1
                                                       facility2
                                                       facility3])

        result1
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_TEST
                           {:search
                            [:fields [:attrs.foo.users.id
                                      :foobar.test]
                             :value "test"]
                            :sort [:id :asc
                                   :some.missing.attr :desc]})

        result2
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_TEST
                           {:search
                            [:fields [:attrs.foo.users.id]
                             :value "bbb"]})

        result3
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_TEST
                           {:search
                            [:fields [:attrs.foo.users.id]
                             :value "4test"]})]

    (is (= #{uuid1 uuid2}
           (set (mapv :id result1))))

    (is (= [uuid3]
           (mapv :id result2)))

    (is (= [uuid2]
           (mapv :id result3)))))

(deftest test-advanced-filter-search-mix

  (let [uuid1
        (uuid/gen)

        uuid2
        (uuid/gen)

        uuid3
        (uuid/gen)

        facility1
        {:id uuid1
         :attrs
         {:state :deleted
          :foo
          [{:users [{:id "4test4@acme.com"}
                    {:id "4test4@acme.com"}]}]}}

        facility2
        {:id uuid2
         :attrs
         {:state :active
          :foo
          [{:users [{:id "3test3@acme.com"}
                    {:id "4test4@acme.com"}]}]}}

        facility3
        {:id uuid3
         :attrs
         {:foo
          [{:users [{:id "AAA@acme.com"}
                    {:id "BBB@acme.com"}]}]}}

        _
        (api/upsert-many *DB* c/TEST_REALM c/SVC_TEST [facility1
                                                       facility2
                                                       facility3])

        result
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_TEST
                           {:filter
                            [:not [:eq :attrs.state :deleted]]
                            :search
                            [:fields [:attrs.foo.users.id]
                             :value "4test4"]})]

    (is (= [uuid2]
           (mapv :id result)))))

(deftest test-import

  (let [ctx
        {}

        uuid1
        (uuid/gen)

        uuid2
        (uuid/gen)

        result
        (with-redefs [import/os-init-scroll
                      (constantly
                       ["token1" [{:id uuid1
                                   :attrs {:foo 1}}
                                  {:id uuid2
                                   :attrs {:foo 2}}]])

                      import/os-call-scroll
                      (constantly
                       ["token2" []])]

          (import/os-import ctx
                            c/TEST_REALM
                            c/SVC_TEST
                            100
                            "15m"))

        entity1
        (api/get-by-id *DB* c/TEST_REALM c/SVC_TEST uuid1)

        entity2
        (api/get-by-id *DB* c/TEST_REALM c/SVC_TEST uuid2)]

    (is (= {:id uuid1
            :attrs {:foo 1}}
           entity1))
    (is (= {:id uuid2
            :attrs {:foo 2}}
           entity2))))

(deftest test-view-store-advanced-search-ok

  (let [ctx
        {:meta {:realm :test}
         :service-name :edd-core
         :view-store :postgres}

        query
        {:filter [:eq :attrs.lms-id "LMSID-1000022665"]}

        uuid1
        (uuid/gen)

        uuid2
        (uuid/gen)

        result
        (search/advanced-search (-> ctx
                                    (assoc :query query)
                                    (with-conn)))]

    (is (= {:total 1, :from 0, :size 1 :has-more? false}
           (dissoc result :hits)))

    (is (= #uuid "9c86240b-64bf-4a7a-a48a-f27208194835"
           (-> result :hits first :id)))))

(deftest test-upsert-bulk

  (let [uuid1
        (uuid/gen)

        uuid2
        (uuid/gen)

        result
        (api/upsert-bulk *DB*
                         c/TEST_REALM c/SVC_TEST
                         [{:id uuid1
                           :attrs {:aaa 1}}
                          {:id uuid2
                           :attrs {:aaa 2}}])]

    (is (= [1 1] result))))

(deftest test-view-store-advanced-search-empty-where

  (let [ctx
        {:meta {:realm :test}
         :service-name :edd-core
         :view-store :postgres}

        query
        {:sort [:attrs.fat1 :desc]}

        result
        (search/advanced-search (-> ctx
                                    (assoc :query query)
                                    (with-conn)))]

    (is (= {:total 51, :from 0, :size 50 :has-more? true}
           (dissoc result :hits)))))

(deftest test-list-options-for-one-field

  (let [ctx
        {:meta {:realm :test}
         :service-name :edd-core
         :view-store :postgres}

        table
        :test_glms_dimension_svc.mv-options_one_field
        _
        (honey/insert *DB*
                      table
                      [{:field_key "attrs.foo.bar"
                        :field_val "foo"}
                       {:field_key "attrs.test"
                        :field_val "abc"}
                       {:field_key "attrs.fat1"
                        :field_val "FAT1"}
                       {:field_key "attrs.fat1"
                        :field_val "FAT2"}])

        result
        (dimension/ctx-list-options-one-field (with-conn ctx)
                                              :attrs.fat1)]

    (is (= {:options
            #{{:attrs.fat1 "FAT2"}
              {:attrs.fat1 "FAT1"}}}
           (update result :options set)))))

(deftest test-list-options-for-two-fields

  (let [ctx
        {:meta {:realm :test}
         :service-name :edd-core
         :view-store :postgres}

        table
        :test_glms_dimension_svc.mv-options_two_fields

        _
        (honey/insert *DB*
                      table
                      [{:field1_key "attrs.foo.bar"
                        :field1_val "foo"
                        :field2_key "attrs.fat2"
                        :field2_val "foo"}
                       {:field1_key "attrs.fat1"
                        :field1_val "abc"
                        :field2_key "dunno"
                        :field2_val "test"}
                       {:field1_key "attrs.fat1"
                        :field1_val "FAT1"
                        :field2_key "attrs.fat2"
                        :field2_val "AAA"}
                       {:field1_key "attrs.fat1"
                        :field1_val "FAT1"
                        :field2_key "attrs.fat2"
                        :field2_val "BBB"}])

        result
        (dimension/ctx-list-options-two-fields (with-conn ctx)
                                               :attrs.fat1
                                               :attrs.fat2)]

    (is (= {:options
            #{{:attrs.fat1 "FAT1", :attrs.fat2 "BBB"}
              {:attrs.fat1 "FAT1", :attrs.fat2 "AAA"}}}
           (update result :options set)))))

(deftest test-advanced-search-ccn-short-name-case

  (let [uuid1
        (uuid/gen)

        app1
        {:id uuid1
         :attrs
         {:cocunut "123456"
          :short-name "Acme Inc"}}

        _
        (api/upsert-many *DB* c/TEST_REALM c/SVC_DIMENSION [app1])

        result1
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name :attrs.top-parent-id]
                             :value "123456/Acme Inc"]
                            :sort ["attrs.cocunut" :asc :attrs.short-name "desc"]})

        result2
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name]
                             :value "123456/foo bar dunno"]})

        result3
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name]
                             :value "990023433453453/Acme Inc"]})

        result4
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name]
                             :value "990023433453453/acme inc"]})

        result5
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name]
                             :value "  \t\n123456/acme inc\f\r\t  "]})

        result6
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name]
                             :value "123-456/acme-inc"]})]

    (is (= [app1 app1] result1))
    (is (= [app1] result2))
    (is (= [app1] result3))
    (is (= [app1] result4))
    (is (= [app1 app1] result5))
    (is (= [] result6))))

(deftest test-advanced-search-with-search-fields

  (let [uuid1
        #uuid "3b2fdaff-2bcc-4fea-86a0-e1a38c52876e"

        uuid2
        #uuid "c7a0a370-6101-457f-8a27-3409268025b4"

        uuid3
        #uuid "88be9b3c-ad63-484c-ae6a-1b2e4d95b55e"

        ;; matches both cocunut and top-parent-id search fields
        app1
        {:id uuid1
         :attrs
         {:rank 3
          :cocunut "123456789"
          :top-parent-id "123456789"}}

        app2
        {:id uuid2
         :attrs
         {:rank 2
          :cocunut "foo"
          :top-parent-id "123456789"}}

        app3
        {:id uuid3
         :attrs
         {:rank 1
          :cocunut "bar"
          :top-parent-id "123456789"}}

        _
        (api/upsert-many *DB* c/TEST_REALM c/SVC_DIMENSION
                         [app1 app2 app3])

        result1
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name :attrs.top-parent-id]
                             :value "123456789"]
                            :sort [:attrs.rank :asc]})

        result2
        (api/find-advanced *DB*
                           c/TEST_REALM c/SVC_DIMENSION
                           {:search
                            [:fields [:attrs.cocunut :attrs.short-name :attrs.top-parent-id]
                             :value "123456789"]
                            :from 1
                            :size 1
                            :sort [:attrs.rank :asc]})]

    (testing "
uuid1 is the first because of search priority: coconut match (even though the rank is 3)
uuid3 is the second because rank is 1
uuid2 is the third because rank is 2
uuid1 is the fourth again because we don't remove UNION duplicates at the moment
"
      (is (= [uuid1 uuid3 uuid2 uuid1] (mapv :id result1))))

    (testing "the same as above but shifted due to size/from"
      (is (= [uuid3] (mapv :id result2))))))

(deftest test-advanced-limit-has-more

  (let [uuid1
        (uuid/gen)

        uuid2
        (uuid/gen)

        uuid3
        (uuid/gen)

        uuid4
        (uuid/gen)

        facility1
        {:id uuid1
         :attrs {:rank 1
                 :foobar "AAA"}}

        facility2
        {:id uuid2
         :attrs {:rank 2
                 :foobar "AAA"}}

        facility3
        {:id uuid3
         :attrs {:rank 3
                 :foobar "AAA"}}

        facility4
        {:id uuid4
         :attrs {:rank 4
                 :foobar "AAA"}}
        _
        (api/upsert-many *DB* c/TEST_REALM c/SVC_TEST
                         [facility1
                          facility2
                          facility3
                          facility4])

        query
        {:filter [:= :attrs.foobar "AAA"]
         :sort [:attrs.rank :asc]
         :size 2}

        ctx (->ctx {:query query})

        result1
        (search/advanced-search (assoc-in ctx [:query :from] 0))

        result2
        (search/advanced-search (assoc-in ctx [:query :from] 2))

        result3
        (search/advanced-search (assoc-in ctx [:query :from] 4))

        upd-result
        (fn [result]
          (update result :hits
                  (fn [rows]
                    (set (map :id rows)))))]

    (testing "first two items, total is +1, has more rows"
      (is (= {:total 3
              :size 2
              :from 0
              :hits #{uuid1 uuid2}
              :has-more? true}
             (upd-result result1))))

    (testing "last two items, total hasn't +1, no more rows"
      (is (= {:total 4
              :size 2
              :from 2
              :hits #{uuid3 uuid4}
              :has-more? false}
             (upd-result result2))))

    (testing "completely empty"
      (is (= {:total 4
              :size 0
              :from 4
              :hits #{}
              :has-more? false}
             (upd-result result3))))))

(deftest test-advanced-search-broken-group

  (let [query
        {:filter [:and]
         :size 200
         :from 0}

        ctx (->ctx {:query query})

        result
        (search/advanced-search ctx)]

    (testing "the broken group was ignored"
      (is (= 200 (-> result :hits count))))))
