(ns edd.view-store.postgres.honey-test
  (:require
   [clojure.test :refer [deftest is]]
   [edd.view-store.postgres.honey :as honey]))

(deftest test-json->
  (let [res
        (honey/format
         {:where [:and
                  [:json-> [:. :foo :bar] 42]
                  [:json-> :column "some-key"]
                  [:json-> :test :foo-bar]
                  [:json-> :aaa 'field]]})]

    (is (= ["WHERE (foo.bar -> 42) AND (column -> 'some-key') AND (test -> 'foo-bar') AND (aaa -> 'field')"]
           res))))

(deftest test-json->>
  (let [res
        (honey/format
         {:where [:and
                  [:json->> [:. :foo :bar] 42]
                  [:json->> :column "some-key"]
                  [:json->> :test :foo-bar]
                  [:json->> :aaa 'field]]})]

    (is (= ["WHERE (foo.bar ->> 42) AND (column ->> 'some-key') AND (test ->> 'foo-bar') AND (aaa ->> 'field')"]
           res))))

(deftest test-json#>
  (let [res
        (honey/format
         {:where [:and
                  [:json#> [:. :foo :bar] [:attrs-top "history-test" 'users -42]]]})]

    (is (= ["WHERE (foo.bar #> ARRAY['attrs-top', 'history-test', 'users', -42])"]
           res))))

(deftest test-json#>>
  (let [res
        (honey/format
         {:where [:and
                  [:json#>> [:. :foo :bar] [:attrs-top "history-test" 'users -42]]]})]

    (is (= ["WHERE (foo.bar #>> ARRAY['attrs-top', 'history-test', 'users', -42])"]
           res))))

(deftest test-hyphen-and-underscore
  (let [res
        (honey/format
         {:select [[:foo :user-foo]
                   [:bar :foo/bar]]})]
    (is (= ["SELECT foo AS user_foo, bar AS foo.bar"]
           res))))
