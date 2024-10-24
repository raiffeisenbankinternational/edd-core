(ns edd.postgres.pool
  "
  Common PG utilities to be shared across event- and view-store.
  "
  (:require
   [edd.db :as db]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as jdbc-conn]
   [next.jdbc.protocols :refer [Connectable]])
  (:import
   com.zaxxer.hikari.HikariDataSource
   com.zaxxer.hikari.pool.HikariProxyConnection
   java.io.Closeable
   java.sql.Connection))

(set! *warn-on-reflection* true)

(defn create-pool ^HikariDataSource [ds-spec]
  (jdbc-conn/->pool HikariDataSource ds-spec))

(def create-pool-mem
  "
  A memoized version of `create-pool` that maintains
  a number of pools per each `db-spec`.
  "
  (memoize create-pool))

(def ^:dynamic *DB* nil)

(defmacro with-pool
  "
  Bind the global *DB* variable to a connection pool
  related to the current context (there might be more
  than one pool). The function that creates a pool is
  memoized.
  "
  [[ctx] & body]
  `(binding [*DB* (-> ~ctx
                      (db/init)
                      (create-pool-mem))]
     ~@body))

(defmacro with-conn
  "
  Bind the global *DB* variable to a specific connection
  taken from the pool. Useful when it's needed to share
  the same connection across multiple functions.
  "
  [[] & body]
  `(with-open [conn# (jdbc/get-connection *DB*)]
     (binding [*DB* conn#]
       ~@body)))

(defmacro with-tx
  "
  Bind the global *DB* variablae to a transactional
  connection. Any SQL executions made under the that
  macro will be within the same transaction. Accepts
  the standard `jdbc/with-transaction` options.
  "
  [[& opt] & body]
  `(jdbc/with-transaction [tx# *DB* ~@opt]
     (binding [*DB* tx#]
       ~@body)))

(defn with-init
  "
  Implementation of the `with-init` multimethod.
  Run the `body-fn` function while the `*DB*` var
  is bound to a corresponding connection pool.
  "
  [ctx body-fn]
  (with-pool [ctx]
    (body-fn ctx)))

;;
;; Prevent (jdbc/get-connection src) from a failure
;; when the src is an instance from HikariProxyConnection.
;;

(extend-protocol Connectable

  HikariProxyConnection

  (get-connection ^Connection [this opts]
    this))
