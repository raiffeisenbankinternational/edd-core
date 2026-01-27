(ns edd.view-store.postgres.api-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.parser :as parser]
   [honey.sql :as sql]))

(deftest test-db-schema
  (let [ctx
        {:service-name "foo-bar"
         :meta {:realm "aaa"}}]

    (is (= [[:. :aaa-foo-bar :aggregates]]
           (api/ctx->table ctx)))

    (try
      (api/ctx->table {:meta {:realm :test}})
      (is false)
      (catch Exception e
        (is (= "service-name is not set"
               (ex-message e)))))

    (try
      (api/ctx->table {:service-name "aa"})
      (is false)
      (catch Exception e
        (is (= "realm is not set"
               (ex-message e)))))))

(deftest test->fq-table
  (is (= [[:. :foo-bar-baz :aggregates]]
         (api/->table :foo-bar-baz))))

(deftest test-advanced-search-map
  (let [query
        {:size 100
         :from 42
         :select [:attrs.one
                  :attrs.two]
         :filter [:and
                  [:= :attr.test 42]
                  [:> :attr.hello "2025"]]
         :sort [:attrs.foo :asc :attrs.bar :desc]
         :search [:fields [:id :attrs.cocunut] :value "test"]}

        query-parsed
        (parser/parse-advanced-search! query)

        sql-map
        (api/advanced-search-sql-map :test
                                     :glms-dimension-svc
                                     query-parsed)

        [sql]
        (sql/format sql-map)]

    (is (= (-> "

WITH layers AS (
  SELECT 1 AS rank, id
    FROM test_glms_dimension_svc.aggregates
    WHERE FALSE
  UNION ALL
  SELECT 2 AS rank, id
    FROM test_glms_dimension_svc.aggregates
    WHERE (aggregate #>> ARRAY['attrs', 'cocunut']) = 'test'
  UNION ALL
  SELECT 3 AS rank, id
    FROM test_glms_dimension_svc.aggregates
    WHERE id::text ILIKE '%test%'
  UNION ALL
  SELECT 4 AS rank, id
    FROM test_glms_dimension_svc.aggregates
    WHERE (aggregate #>> ARRAY['attrs', 'cocunut']) ILIKE '%test%'
), layer AS (
  SELECT MIN(rank) AS rank, id
  FROM layers
  GROUP BY id
  ORDER BY 1 ASC
)
SELECT aggregate
FROM layer
INNER JOIN test_glms_dimension_svc.aggregates USING (id)
WHERE
     (aggregate @@ '$.attr.test == 42')
 AND (aggregate @@ '$.attr.hello > \"2025\"')
ORDER BY
  rank ASC,
  (aggregate #>> ARRAY['attrs', 'foo']) ASC,
  (aggregate #>> ARRAY['attrs', 'bar']) DESC
LIMIT 100
OFFSET 42

"
               (str/replace #"(?m)^\s+" "")
               (str/replace #"\n" " ")
               (str/replace "( " "(")
               (str/replace " )" ")")
               (str/trim))
           sql))))
