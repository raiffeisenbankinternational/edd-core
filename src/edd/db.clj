(ns edd.db
  (:require
   [lambda.util :as util]
   [next.jdbc.result-set :as result-set]
   [next.jdbc.prepare :as prepare]
   [clojure.tools.logging :as log])
  (:import
   (clojure.lang IPersistentMap IPersistentVector Keyword)
   (java.sql Date Timestamp)
   (org.postgresql.util PGobject)
   (java.time LocalDate LocalDateTime)
   (org.postgresql.jdbc PgPreparedStatement)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def date-format "dd/MM/yyyy")

(extend-protocol result-set/ReadableColumn
  Date
  (read-column-by-index [^java.sql.Date v _ _]
    (.toLocalDateTime (Timestamp. (.getTime v))))

  Timestamp
  (read-column-by-index [^java.sql.Timestamp v _ _]
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
  (set-parameter [^LocalDateTime v ^PgPreparedStatement s i]
    (.setObject s i
                (Timestamp/valueOf v)))

  Keyword
  (set-parameter [v ^PgPreparedStatement s i]
    (.setObject s i
                (name v)))
  LocalDate
  (set-parameter [^LocalDate v ^PgPreparedStatement s i]
    (.setObject s i
                (Timestamp/valueOf (.atStartOfDay v)))))

(defn init
  [ctx]
  (let [spec {:dbtype                    "postgres"
              :dbname                    "postgres"
              :initializationFailTimeout 0
              :reWriteBatchedInserts     true
              :minimumIdle 1
              :validationTimeout 1000
              :maximumPoolSize 10
              :password                  (get-in ctx [:db :password]
                                                 (util/get-env "DatabasePassword"
                                                               "no-secret"))
              :username                  "postgres"
              :host                      (util/get-env "DatabaseEndpoint" "127.0.0.1")
              :schema                    "postgres"
              :port                      "5432"}]
    (log/info "Initializing postgres event-store: " (dissoc spec :user :username :password))
    spec))

