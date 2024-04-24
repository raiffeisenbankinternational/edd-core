(ns dev
  (:require
   [edd.postgres.const :as c]
   [edd.postgres.entity :as e]
   [edd.postgres.fixtures :as fix]
   [edd.postgres.pool :as pool]
   [lambda.uuid :as uuid]
   [next.jdbc :as jdbc]))

(def DB-OPT
  {:dbtype "postgres"
   :dbname "test"
   :port 15432
   :host "127.0.0.1"
   :user "test"
   :password "test"})

(defn insert-fake-rows [db limit]

  (let [rows
        (for [x (range 0 limit)]
          {:id (uuid/gen)
           :attrs {:field-0 (format "field-0-%s" x)
                   :field-1 (format "field-1-%s" x)
                   :field-2 (format "field-2-%s" x)
                   :field-3 (format "field-3-%s" x)
                   :field-4 (format "field-4-%s" x)
                   :field-5 (format "field-5-%s" x)
                   :field-6 (format "field-6-%s" x)
                   :field-7 (format "field-7-%s" x)
                   :field-8 (format "field-8-%s" x)
                   :field-9 (format "field-9-%s" x)}})]

    (e/upsert-many db c/TEST_SCHEMA rows)))


(comment
  (with-open [conn (jdbc/get-connection DB-OPT)]
    (insert-fake-rows conn 100000)))
