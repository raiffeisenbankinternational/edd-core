(ns edd.db
  (:require
   [lambda.util :as util]
   [next.jdbc.result-set :as result-set]
   [next.jdbc.prepare :as prepare])
  (:import
   (clojure.lang IPersistentMap IPersistentVector Keyword)
   (java.sql Date Timestamp)
   (org.postgresql.util PGobject)
   (java.time LocalDate LocalDateTime)
   (org.postgresql.jdbc PgPreparedStatement)))

(def date-format "dd/MM/yyyy")

(extend-protocol result-set/ReadableColumn
  Date
  (read-column-by-index [v _ _]
    (.toLocalDateTime v))

  Timestamp
  (read-column-by-index [v _ _]
    (.toLocalDateTime v))

  PGobject
  (read-column-by-index [pgobj _metadata _index]
    (let [type (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "jsonb" (util/to-edn value)
        value))))

(defn to-pg-json
  [value]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (util/to-json value))))

(extend-protocol prepare/SettableParameter
  IPersistentMap
  (set-parameter [v ^PgPreparedStatement ps ^long i]
    (.setObject ps i (to-pg-json v)))

  IPersistentVector
  (set-parameter [v ps ^long i]
    (.setObject ^PgPreparedStatement ps i (to-pg-json v)))

  LocalDateTime
  (set-parameter [v ^PgPreparedStatement s i]
    (.setObject s i
                (Timestamp/from v)))

  Keyword
  (set-parameter [v ^PgPreparedStatement s i]
    (.setObject s i
                (name v)))
  LocalDate
  (set-parameter [v ^PgPreparedStatement s i]
    (.setObject s i
                (Timestamp/from v))))

(defn init
  [ctx]
  (assoc ctx :ds {:dbtype                "postgres"
                  :dbname                "postgres"
                  :reWriteBatchedInserts true
                  :password              (get-in ctx [:db :password]
                                                 (util/get-env "DatabasePassword"
                                                               "no-secret"))
                  :user                  "postgres"
                  :host                  (util/get-env "DatabaseEndpoint" "127.0.0.1")
                  :schema                "postgres"
                  :post                  "5432"}))

