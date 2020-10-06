(ns edd.db
  (:require
   [lambda.util :as util]
   [next.jdbc.result-set :as result-set]
   [next.jdbc.prepare :as prepare]
   [clojure.tools.logging :as log]
   [aws :as aws])
  (:import
   (clojure.lang IPersistentMap IPersistentVector)
   (java.sql Date Timestamp PreparedStatement)
   (org.postgresql.util PGobject)
   (java.time LocalDate LocalDateTime OffsetDateTime ZoneOffset)

   (java.time.format DateTimeFormatter)
   (org.postgresql.jdbc PgPreparedStatement)
   (java.util UUID)))

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

  LocalDate
  (set-parameter [v ^PgPreparedStatement s i]
    (.setObject s i
                (Timestamp/from v))))

(defn db_host
  []
  (if (System/getenv "PrivateHostedZoneName")
    (str "rds-" (System/getenv "DatabaseName") "." (System/getenv "PrivateHostedZoneName"))
    "127.0.0.1"))

(defn db_pwd
  [ctx]
  (get-in ctx [:db :password]))

(defn ds
  [ctx]
  (assoc ctx
         :ds {:dbtype                "postgres"
              :dbname                "postgres"
              :reWriteBatchedInserts true
              :password              (db_pwd ctx)
              :user                  "postgres"
              :host                  (db_host)
              :schema                "glms"
              :post                  "5432"}))

(defn init
  [ctx]
  (-> ctx
      (merge (util/load-config "secret.json"))
      (ds)))

