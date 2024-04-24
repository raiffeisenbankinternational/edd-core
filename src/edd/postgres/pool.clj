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

(def memoized-create-pool (memoize create-pool))

(defn with-init
  "
  A common init function for both event- and view- stores.
  "
  [ctx body-fn]
  (let [connection (-> ctx
                       db/init
                       memoized-create-pool
                       jdbc/get-connection
                       delay)]
    (let [ctx-with-conn
          (if (contains? ctx :con)
            ctx
            (assoc ctx :con connection))]
      (try
        (body-fn ctx-with-conn)
        (finally
          (when (realized? connection)
            (let [^HikariProxyConnection conn @connection]
              (.close conn))))))))

(defmacro with-conn
  "
  Execute a block of code binding the leading symbol
  to the newly acquired connection from the pool.
  "
  [[bind ctx] & body]
  `(with-open [~bind (-> ~ctx
                         db/init
                         memoized-create-pool
                         jdbc/get-connection)]
     ~@body))

(defn ->conn
  "
  Get the current connection from the context.
  "
  [ctx]
  (-> ctx :con deref))

;;
;; Prevent (jdbc/get-connection src) from a failure
;; when the src is an instance from HikariProxyConnection.
;;

(extend-protocol Connectable

  HikariProxyConnection

  (get-connection ^Connection [this opts]
    this))
