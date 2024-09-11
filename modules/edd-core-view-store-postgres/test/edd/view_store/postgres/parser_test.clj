(ns edd.view-store.postgres.parser-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.parser :as parser]))

(defmacro is-parsing-error [& body]
  `(try
     ~@body
     (is false "should not have been reached")
     (catch Throwable e#
       (is (= "Could not parse OS query"
              (ex-message e#))))))

(defn filter->result
  ([filter]
   (filter->result c/SVC_TEST filter))

  ([service filter]
   (->> filter
        (parser/filter->where service)
        (honey/format)
        (first))))

(deftest test-dimension-attrs
  (testing "plain case"
    (let [filter
          [:and
           [:= :state :test]
           [:= "attrs.cocunut" "abc"]
           [:= :attrs.short-name "hello"]
           [:= :attrs.foo-bar "hello"]]]
      (is (= "((aggregate #>> ARRAY['state']) = ':test') AND ((aggregate #>> ARRAY['attrs', 'cocunut']) = 'abc') AND (aggregate @@ '$.attrs.\"short-name\" == \"hello\"') AND (aggregate @@ '$.attrs.\"foo-bar\" == \"hello\"')"
             (filter->result c/SVC_DIMENSION filter)))))
  (testing "in case"
    (let [filter
          [:and
           [:in "attrs.cocunut" ["test" :foo 'bar 42]]]]
      (is (= "((aggregate #>> ARRAY['attrs', 'cocunut']) IN ('test', ':foo', 'bar', '42'))"
             (filter->result c/SVC_DIMENSION filter))))))

(deftest test-notification-attrs
  (let [filter
        [:and
         [:= :attrs.status :created]
         [:= "attrs.user.id" "test@test.com"]
         [:= :attrs.short-name "hello"]
         [:= :attrs.user.role "admin"]]]
    (is (= "((aggregate #>> ARRAY['attrs', 'status']) = ':created') AND ((aggregate #>> ARRAY['attrs', 'user', 'id']) = 'test@test.com') AND (aggregate @@ '$.attrs.\"short-name\" == \"hello\"') AND ((aggregate #>> ARRAY['attrs', 'user', 'role']) = 'admin')"
           (filter->result :glms-notification-svc filter)))))

(deftest test-parse-os-query-basic

  (testing "a single predicate"
    (let [query
          [:eq :foo.bar.baz 42]
          result
          (parser/parse-filter! query)]

      (is (= [:predicate
              [:predicate-simple {:op :eq
                                  :attr :foo.bar.baz
                                  :value 42}]]
             result))))

  (testing "boken group (UI case)"
    (let [query
          [:and [:= :foo 42] [:or]]

          result
          (parser/parse-filter! query)]

      (is (= [:group-variadic
              {:condition :and,
               :children
               [[:predicate [:predicate-simple {:op :=, :attr :foo, :value 42}]]
                [:group-broken [:or]]]}]
             result))))

  (testing "in predicate but value is not sequential"
    (let [query
          [:in :foo.bar.baz 42]]
      (try
        (parser/parse-filter! query)
        (is false)
        (catch Exception e
          (is e)))))

  (testing "the attr is a string"
    (let [query
          [:eq "foo.bar.baz" 42]
          result
          (parser/parse-filter! query)]
      (is (= [:predicate
              [:predicate-simple {:op :eq
                                  :attr "foo.bar.baz"
                                  :value 42}]]
             result))))

  (testing "a simple AND group"
    (let [query
          [:and
           [:= :hello.test "foo"]
           [:eq "foo.bar.baz" 42]]
          result
          (parser/parse-filter! query)]
      (is (= [:group-variadic
              {:condition :and
               :children
               [[:predicate
                 [:predicate-simple {:op :=
                                     :attr :hello.test
                                     :value "foo"}]]
                [:predicate
                 [:predicate-simple {:op :eq
                                     :attr "foo.bar.baz"
                                     :value 42}]]]}]
             result))))

  (testing "a simple OR group"
    (let [query
          [:or
           [:= :hello.test "foo"]
           [:eq "foo.bar.baz" 42]]
          result
          (parser/parse-filter! query)]
      (is (= [:group-variadic
              {:condition :or
               :children
               [[:predicate
                 [:predicate-simple {:op := :attr :hello.test :value "foo"}]]
                [:predicate
                 [:predicate-simple {:op :eq :attr "foo.bar.baz" :value 42}]]]}]
             result))))

  (testing "nested logical groups"
    (let [query
          [:or
           [:and
            [:= :attrs.field1 1]
            [:= :attrs.field2 2]]
           [:and
            [:= :attrs.field3 3]
            [:= :attrs.field4 4]]]
          result
          (parser/parse-filter! query)]
      (is (= [:group-variadic
              {:condition :or
               :children
               [[:group-variadic
                 {:condition :and
                  :children
                  [[:predicate
                    [:predicate-simple {:op := :attr :attrs.field1 :value 1}]]
                   [:predicate
                    [:predicate-simple {:op := :attr :attrs.field2 :value 2}]]]}]
                [:group-variadic
                 {:condition :and
                  :children
                  [[:predicate
                    [:predicate-simple {:op := :attr :attrs.field3 :value 3}]]
                   [:predicate
                    [:predicate-simple
                     {:op := :attr :attrs.field4 :value 4}]]]}]]}]
             result)))))

(deftest test-parse-advanced-search

  (testing "a simple case"
    (let [query
          {:filter [:eq :foo.bar 42]
           :search [:fields [:aa.bb :cc.dd] :value "hello"]
           :sort [:test.hello :desc]
           :size 100
           :from "123"}

          query-parsed
          (parser/parse-advanced-search! query)]

      (is (= {:filter
              [:predicate [:predicate-simple {:op :eq, :attr :foo.bar, :value 42}]]
              :search
              {:_1 :fields, :attrs [:aa.bb :cc.dd], :_2 :value, :value "hello"}
              :sort [{:attr :test.hello, :order :desc}]
              :size [:integer 100]
              :from [:string "123"]}
             query-parsed)))))

(deftest test-parse-dimension-attrs-cocunut

  (testing "filter part"
    (let [filter
          [:and
           [:eq :attrs.cocunut "abc"]
           [:not [:eq :state :deleted]]]]

      (is (= "((aggregate #>> ARRAY['attrs', 'cocunut']) = 'abc') AND NOT ((aggregate #>> ARRAY['state']) = ':deleted')"
             (filter->result c/SVC_DIMENSION filter))))))

(deftest test-parse-os-query-nested-attrs

  (testing "the nested operator"
    (let [query
          [:nested :doc.users
           [:and
            [:> :id 42]
            [:== :name "test"]]]
          result
          (parser/parse-filter! query)]
      (is (= [:nested
              {:tag :nested,
               :attr :doc.users,
               :group
               {:condition :and,
                :children
                [[:predicate-simple {:op :>, :attr :id, :value 42}]
                 [:predicate-simple {:op :==, :attr :name, :value "test"}]]}}]
             result))))

  (testing "nested cannot have a predicate"
    (let [query
          [:nested :doc.users
           [:eq :id 42]]]
      (try
        (parser/parse-filter! query)
        (is false)
        (catch Exception e
          (is e)))))

  (testing "the nested in a group"
    (let [query
          [:and
           [:= :foo.bar "123"]
           [:nested :doc.users
            [:and
             [:eq :id 42]
             [:= :name "foo"]]]]
          result
          (parser/parse-filter! query)]
      (is (= [:group-variadic
              {:condition :and,
               :children
               [[:predicate
                 [:predicate-simple {:op :=, :attr :foo.bar, :value "123"}]]
                [:nested
                 {:tag :nested,
                  :attr :doc.users,
                  :group
                  {:condition :and,
                   :children
                   [[:predicate-simple {:op :eq, :attr :id, :value 42}]
                    [:predicate-simple {:op :=, :attr :name, :value "foo"}]]}}]]}]
             result)))))

(deftest test-attrs-appication-id=string

  (let [filter
        [:and
         [:eq :attrs.application-id "123123"]
         [:in :attrs.name ["foo" "bar"]]
         [:in :attrs.test ["hello"]]
         [:wildcard :attrs.application-id "003"]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'application-id']) = '123123') AND (aggregate @?? '$.attrs.name  ?  ((@ == \"foo\") || (@ == \"bar\"))') AND (aggregate @@ '$.attrs.test == \"hello\"') AND ((aggregate #>> ARRAY['attrs', 'application-id']) ILIKE '%003%')"
           (filter->result c/SVC_APPLICATION filter)))))

(deftest test-honey-where

  (testing "a simple and group with eq and in"
    (let [filter
          [:and
           [:eq :id 42]
           [:in :name ["foo" "bar"]]]]

      (is (= "(aggregate @@ '$.id == 42') AND (aggregate @?? '$.name  ?  ((@ == \"foo\") || (@ == \"bar\"))')"
             (filter->result filter)))))

  (testing "a complex group"
    (let [filter
          [:or
           [:and
            [:= :foo.some-attr 1]
            [:eq :foo.baz nil]]
           [:and
            [:in :test.user-name [true false]]
            [:in "test.bbb" ["aa" "bb"]]]]]

      (is (= "((aggregate @@ '$.foo.\"some-attr\" == 1') AND (aggregate @@ '$.foo.baz == null')) OR ((aggregate @?? '$.test.\"user-name\"  ?  ((@ == true) || (@ == false))') AND (aggregate @?? '$.test.bbb  ?  ((@ == \"aa\") || (@ == \"bb\"))'))"
             (filter->result filter)))))

  (testing "nested operator"
    (let [filter
          [:or
           [:eq :attrs.foo 1]
           [:nested :attrs.users
            [:and
             [:= :attrs.users.id 1]
             [:= :attrs.users.name "smith"]]]]]

      (is (= "(aggregate @@ '$.attrs.foo == 1') OR (aggregate @?? '$.attrs.users  ?  ((@.id == 1) && (@.name == \"smith\"))')"
             (filter->result filter)))))

  (testing "check SQL injections (quoting)"
    (let [weird-line
          "!@#$%^&*()'test\"\r\f\thello'~$_?drop table --'''students??;"

          filter
          [:or
           [:eq :attrs.foo weird-line]
           [:nested :attrs.users
            [:and
             [:= :attrs.users.id 1]
             [:= :attrs.users.name weird-line]]]]]

      (is (= "(aggregate @@ '$.attrs.foo == \"!@#$%^&*()''test\\\"\\r\\f\\thello''~$_?drop table --''''''students??;\"') OR (aggregate @?? '$.attrs.users  ?  ((@.id == 1) && (@.name == \"!@#$%^&*()''test\\\"\\r\\f\\thello''~$_?drop table --''''''students??;\"))')"
             (filter->result filter))))))

(deftest test-honey-where-gte-lte

  (let [filter
        [:and
         [:< :id 1]
         [:<= :id 1]
         [:= :id 1]
         [:eq :id 1]
         [:== :id 1]
         [:>= :id 1]
         [:> :id 1]]]

    (is (= "(aggregate @@ '$.id < 1') AND (aggregate @@ '$.id <= 1') AND (aggregate @@ '$.id == 1') AND (aggregate @@ '$.id == 1') AND (aggregate @@ '$.id == 1') AND (aggregate @@ '$.id >= 1') AND (aggregate @@ '$.id > 1')"
           (filter->result filter)))))

(deftest test-honey-where-exists

  (let [filter
        [:and
         [:exists "foo.bar.baz"]]]

    (is (= "(aggregate @?? '$.foo.bar.baz')"
           (filter->result filter)))))

(deftest test-parser-eq-uuid

  (let [uuid
        #uuid "e5e4048f-1f82-4a84-be2f-a85300c853ab"

        filter
        [:eq :id uuid]]

    (is (= "id = 'e5e4048f-1f82-4a84-be2f-a85300c853ab'"
           (filter->result filter)))))

(deftest test-parser-in-uuid

  (testing "singular"
    (let [uuid
          #uuid "e5e4048f-1f82-4a84-be2f-a85300c853ab"

          filter
          [:in :id [uuid]]]

      (is (= "id = 'e5e4048f-1f82-4a84-be2f-a85300c853ab'"
             (filter->result filter)))))

  (testing "singular"
    (let [uuid1
          #uuid "e5e4048f-1f82-4a84-be2f-a85300c853ab"

          uuid2
          #uuid "e0fe0037-3d22-4dc4-a169-d89859f48378"

          filter
          [:in :id [uuid1 uuid2]]]

      (is (= "id IN ('e5e4048f-1f82-4a84-be2f-a85300c853ab', 'e0fe0037-3d22-4dc4-a169-d89859f48378')"
             (filter->result filter))))))

(deftest test-honey-where-not

  (let [filter
        [:not
         [:and
          [:exists "foo.bar.baz"]
          [:= "foo.bar.baz" 1]]]]

    (is (= "NOT ((aggregate @?? '$.foo.bar.baz') AND (aggregate @@ '$.foo.bar.baz == 1'))"
           (filter->result filter)))))

(deftest test-honey-where-wildcard

  (let [filter
        [:and
         [:wildcard :aa.bb "foo"]
         [:wildcard :aa.cc "bar"]]]

    (is (= "(aggregate @@ '($.aa.bb like_regex \"foo\" flag \"iq\")') AND (aggregate @@ '($.aa.cc like_regex \"bar\" flag \"iq\")')"
           (filter->result filter)))))

(deftest test-filter-integration-tests

  (let [filter
        [:and
         [:in :attrs.risk-on-id #{#uuid "390ec691-cd69-49fa-b80a-408b861b55a4"}]
         [:eq :attrs.risk-taker-id #uuid "b0000000-0000-0000-0000-000000826673"]
         [:eq :attrs.fat1 "TCMG"]
         [:eq :attrs.fat2 'TCM12]
         [:in :attrs.status #{:approved :activated}]
         [:in :attrs.facility-type #{:tcmg-facility}]]]

    (is (= "(aggregate @@ '$.attrs.\"risk-on-id\" == \"#390ec691-cd69-49fa-b80a-408b861b55a4\"') AND (aggregate @@ '$.attrs.\"risk-taker-id\" == \"#b0000000-0000-0000-0000-000000826673\"') AND (aggregate @@ '$.attrs.fat1 == \"TCMG\"') AND (aggregate @@ '$.attrs.fat2 == \"TCM12\"') AND (aggregate @?? '$.attrs.status  ?  ((@ == \":approved\") || (@ == \":activated\"))') AND (aggregate @@ '$.attrs.\"facility-type\" == \":tcmg-facility\"')"
           (filter->result filter)))))

(deftest test-filter-broken-group
  (let [filter
        [:or [:= :foo 1] [:and]]]
    (is (= "(aggregate @@ '$.foo == 1') OR FALSE"
           (filter->result filter))))
  (let [filter [:and]]
    (is (= "FALSE"
           (filter->result filter)))))

(deftest test-parse-os-group-array

  (testing "test group array"
    (let [filter
          [:and
           [[:= :attrs.foo 1]
            [:= :attrs.bar 2]
            [:or
             [[:> :attrs.aaa 3]
              [:< :attrs.bbb 3]
              [:wildcard :attrs.name "hello"]]]]]]

      (is (= "(aggregate @@ '$.attrs.foo == 1') AND (aggregate @@ '$.attrs.bar == 2') AND ((aggregate @@ '$.attrs.aaa > 3') OR (aggregate @@ '$.attrs.bbb < 3') OR (aggregate @@ '($.attrs.name like_regex \"hello\" flag \"iq\")'))"
             (filter->result filter))))))

(deftest test-parse-os-wildcard-id-uuid

  (testing "test group array"
    (let [uuid
          #uuid "ead78109-af06-4c66-805e-5a9914b1123f"

          filter
          [:and
           [[:= :attrs.foo 1]
            [:wildcard "id" uuid]
            [:eq "id" uuid]]]]

      (is (= "(aggregate @@ '$.attrs.foo == 1') AND (id = 'ead78109-af06-4c66-805e-5a9914b1123f') AND (id = 'ead78109-af06-4c66-805e-5a9914b1123f')"
             (filter->result filter))))))

(deftest test-parse-os-attrs-status-weird

  (testing "test group array"
    (let [filter
          [:and
           [[:= :attrs.foo 1]
            [:eq :attrs.status :active :pending]]]]

      (is (= "(aggregate @@ '$.attrs.foo == 1') AND (aggregate @?? '$.attrs.status  ?  ((@ == \":active\") || (@ == \":pending\"))')"
             (filter->result filter))))))

(deftest test-currency-fields
  (let [filter
        [:and
         [:<= :business-date "2023-01-02"]]]
    (is (= "((aggregate #>> ARRAY['business-date']) <= '2023-01-02')"
           (filter->result c/SVC_CURRENCY filter)))))

(deftest test-parse-os-nested-wildcard

  (let [filter
        [:nested "attrs.assignees.users"
         [:and
          [:eq :attrs.assignees.users.role :lime-risk-managers]
          [:wildcard :attrs.assignees.users.status "name"]]]]

    (is (= "aggregate @?? '$.attrs.assignees.users  ?  ((@.role == \":lime-risk-managers\") && (@.status like_regex \"name\" flag \"iq\"))'"
           (filter->result filter)))))

(deftest test-wildcard-application-id

  (let [filter
        [:or
         [:= :attrs.application-id "123"]
         [:= :attrs.application-id 456]
         [:wildcard :attrs.application-id "123"]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'application-id']) = '123') OR ((aggregate #>> ARRAY['attrs', 'application-id']) = '456') OR ((aggregate #>> ARRAY['attrs', 'application-id']) ILIKE '%123%')"
           (filter->result c/SVC_APPLICATION filter)))))

(deftest test-wildcard-application-id-ui-case

  (let [filter
        [:and
         [:wildcard "attrs.application-id" "21150"]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'application-id']) ILIKE '%21150%')"
           (filter->result c/SVC_APPLICATION filter)))))

(deftest test-wildcard-application-id-keyword-task-service
  (let [filter
        [:and
         [:wildcard "attrs.application-id.keyword" "21150"]
         [:wildcard "attrs.application.application-id.keyword" "21150"]]]
    (is (= "(aggregate @@ '($.attrs.\"application-id\" like_regex \"21150\" flag \"iq\")') AND ((aggregate #>> ARRAY['attrs', 'application', 'application-id']) ILIKE '%21150%')"
           (filter->result :glms-task-manager-svc filter)))))

(deftest test-wildcard-application-id-keyword-in

  (let [filter
        [:and
         [:in :attrs.application-id.keyword ["1" 2 "3" :dunno]]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'application-id']) IN ('1', '2', '3', ':dunno'))"
           (filter->result c/SVC_APPLICATION filter)))))

(deftest test-in-empty-vector

  (let [filter
        [:in :some.attr []]]

    (try
      (filter->result filter)
      (is false)
      (catch clojure.lang.ExceptionInfo e
        (is (= "could not parse OS filter DSL"
               (ex-message e)))
        (is (= {:message "could not parse OS filter DSL", :data [:in :some.attr []]}
               (ex-data e)))))))

(deftest test-wildcard-application-id-uuid-case
  ;; feel free to drop this test once LSP-7606 is done
  (let [uuid
        #uuid "e279980e-38ea-4104-8328-ead214472551"

        filter
        [:or
         [:= :attrs.application-id uuid]
         [:= :attrs.application-id 123]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'application-id']) = '#e279980e-38ea-4104-8328-ead214472551') OR ((aggregate #>> ARRAY['attrs', 'application-id']) = '123')"
           (filter->result c/SVC_APPLICATION filter)))))

(deftest test-application-id-in

  (let [filter
        [:or
         [:in :attrs.application-id [1 "2" 3 "4" 5]]
         [:in :attrs.application-id.keyword [1 "2" 3 "4" 5]]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'application-id']) IN ('1', '2', '3', '4', '5')) OR ((aggregate #>> ARRAY['attrs', 'application-id']) IN ('1', '2', '3', '4', '5'))"
           (filter->result c/SVC_APPLICATION filter)))))

(deftest test-trailing-keyword-removed

  (let [filter
        [:or
         [:= :foo.bar.keyword "123"]
         [:= "foo.keyword.foo" "123"]
         [:wildcard :attrs.application-id.keyword "123"]]]

    (is (= "(aggregate @@ '$.foo.bar == \"123\"') OR (aggregate @@ '$.foo.keyword.foo == \"123\"') OR (aggregate @@ '($.attrs.\"application-id\" like_regex \"123\" flag \"iq\")')"
           (filter->result filter)))))

(deftest test-attrs-creation-time-1

  (let [filter
        [:and
         [:lte :attrs.creation-time "2024-05-09"]
         [:gte :attrs.creation-time "2024-05-09"]]]

    (is (= "(aggregate @@ '$.attrs.\"creation-time\" <= \"2024-05-09T23:59:59.999Z\"') AND (aggregate @@ '$.attrs.\"creation-time\" >= \"2024-05-09T00:00:00.000Z\"')"
           (filter->result filter)))))

(deftest test-attrs-creation-time-2

  (let [filter
        [:and
         [:lte :creation-time "2024-05-10"]
         [:gte :creation-time "2024-05-09"]]]

    (is (= "(aggregate @@ '$.\"creation-time\" <= \"2024-05-10T23:59:59.999Z\"') AND (aggregate @@ '$.\"creation-time\" >= \"2024-05-09T00:00:00.000Z\"')"
           (filter->result filter)))))

(deftest test-attrs-creation-time-3

  (let [filter
        [:and
         [:<= :attrs.resolution-date "2024-05-10"]
         [">" :attrs.resolution-date "2024-05-09"]]]

    (is (= "(aggregate @@ '$.attrs.\"resolution-date\" <= \"2024-05-10T23:59:59.999Z\"') AND (aggregate @@ '$.attrs.\"resolution-date\" > \"2024-05-09T00:00:00.000Z\"')"
           (filter->result filter)))))

(deftest test-in-asset-class-code

  (let [filter
        [:and
         [:in "attrs.risk-on.asset-class.asset-class-code" [42]]
         [:in :attrs.risk-on.asset-class.asset-class-code [1 2 3]]]]

    (is (= "(aggregate @@ '$.attrs.\"risk-on\".\"asset-class\".\"asset-class-code\" == \"42\"') AND (aggregate @?? '$.attrs.\"risk-on\".\"asset-class\".\"asset-class-code\"  ?  ((@ == \"1\") || (@ == \"2\") || (@ == \"3\"))')"
           (filter->result filter)))))

(deftest test-sort-by

  (let [search
        [:attrs.some.name :asc
         "attrs.foo.test" :desc]

        result
        (honey/format {:select [:*]
                       :order-by (parser/sort->order-by :test search)})]

    (is (= ["SELECT * ORDER BY (aggregate #>> ARRAY['attrs', 'some', 'name']) ASC, (aggregate #>> ARRAY['attrs', 'foo', 'test']) DESC"]
           result))))

(deftest test-sort-by-dimension-cocunut

  (let [search
        ["attrs.cocunut" :asc]

        result
        (honey/format {:select [:*]
                       :order-by (parser/sort->order-by :test search)})]

    (is (= ["SELECT * ORDER BY (aggregate #>> ARRAY['attrs', 'cocunut']) ASC"]
           result))))

(deftest test-sort-by-application-id-no-service

  (let [search
        ["attrs.application-id" :asc]

        result
        (honey/format {:select [:*]
                       :order-by (parser/sort->order-by :test search)})]

    (is (= ["SELECT * ORDER BY (aggregate #>> ARRAY['attrs', 'application-id']) ASC"]
           result))))

(deftest test-sort-by-application-id-with-service

  (let [search
        ["attrs.application-id" :asc]

        clause
        (parser/sort->order-by c/SVC_APPLICATION search)

        result
        (honey/format {:select [:*]
                       :order-by clause})]

    (is (= ["SELECT * ORDER BY CAST((aggregate #>> ARRAY['attrs', 'application-id']) AS INT) ASC"]
           result))))

(deftest test-sort-by-application-id-task-manager

  (let [search
        ["attrs.application.application-id" :asc-number]

        clause
        (parser/sort->order-by c/SVC_TASK_MANAGER search)

        result
        (honey/format {:select [:*]
                       :order-by clause})]

    (is (= ["SELECT * ORDER BY CAST((aggregate #>> ARRAY['attrs', 'application', 'application-id']) AS INT) ASC"]
           result))))

(deftest test-sort-by-application-id-non-task-manager-service

  (let [search
        ["attrs.application.application-id" :asc-number]

        clause
        (parser/sort->order-by :test search)

        result
        (honey/format {:select [:*]
                       :order-by clause})]

    (is (= ["SELECT * ORDER BY (aggregate #>> ARRAY['attrs', 'application', 'application-id']) ASC"]
           result))))

(deftest test-sort-by-empty

  (try
    (parser/sort->order-by :test [])
    (is false)
    (catch Exception e
      (is (= "could not parse OS sort DSL"
             (ex-message e)))))

  (try
    (parser/sort->order-by :test [:some.attrs :unknown])
    (is false)
    (catch Exception e
      (is (= "could not parse OS sort DSL"
             (ex-message e))))))

(deftest test-sort-by-attrs-creation-time

  (let [search
        [:attrs.creation-time :desc]

        clause
        (parser/sort->order-by :test search)

        result
        (honey/format {:select [:*]
                       :order-by clause})]

    (is (= ["SELECT * ORDER BY created_at DESC"]
           result))))

(deftest test-sort-by-creation-time

  (let [search
        [:creation-time :desc-number]

        clause
        (parser/sort->order-by :test search)

        result
        (honey/format {:select [:*]
                       :order-by clause})]

    (is (= ["SELECT * ORDER BY created_at DESC"]
           result))))

(deftest test-dimension-wildcard

  (let [filter
        [:or
         [:wildcard :attrs.cocunut "123"]
         [:wildcard :attrs.short-name "123"]
         [:wildcard :attrs.top-parent-id "123"]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'cocunut']) ILIKE '%123%') OR ((aggregate #>> ARRAY['attrs', 'short-name']) ILIKE '%123%') OR ((aggregate #>> ARRAY['attrs', 'top-parent-id']) ILIKE '%123%')"
           (filter->result c/SVC_DIMENSION filter)))))

(deftest test-dimension-wildcard-with-slash

  (let [filter
        [:or
         [:= :attrs.cocunut "123/abc/test"]
         [:= :attrs.short-name "123/abc/test"]
         [:= :attrs.top-parent-id "123/abc/test"]
         [:wildcard :attrs.cocunut "123/abc/test"]
         [:wildcard :attrs.short-name "123/abc/test"]
         [:wildcard :attrs.top-parent-id "123/abc/test"]]]

    (is (= "((aggregate #>> ARRAY['attrs', 'cocunut']) = '123/abc/test') OR (aggregate @@ '$.attrs.\"short-name\" == \"123/abc/test\"') OR ((aggregate #>> ARRAY['attrs', 'top-parent-id']) = '123/abc/test') OR ((aggregate #>> ARRAY['attrs', 'cocunut']) ILIKE '%123%') OR ((aggregate #>> ARRAY['attrs', 'short-name']) ILIKE '%abc/test%') OR ((aggregate #>> ARRAY['attrs', 'top-parent-id']) ILIKE '%123/abc/test%')"
           (filter->result c/SVC_DIMENSION filter)))))
