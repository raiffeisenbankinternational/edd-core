(ns edd.view-store.postgres.jdbc
  (:import
   (org.postgresql.util PGobject)
   (java.sql ResultSet))
  (:require
   [lambda.util :as util]
   [edd.view-store.postgres.attrs :as attrs]
   [next.jdbc.result-set :as jdbc.rs]))

;;
;; A special kind of builder that reads a single 'aggregate'
;; column on the fly in one pass. Faster than wrapping the
;; result with map(v).
;;

(deftype AggregateBuilder [^ResultSet rs]

  jdbc.rs/RowBuilder

  (->row [this]
    (.getObject rs "aggregate"))

  (column-count [this]
    1)

  (with-column [this row i]
    row)

  (with-column-value [this row col v]
    row)

  (row! [this row]
    row)

  jdbc.rs/ResultSetBuilder

  (->rs [this]
    (transient []))

  (with-row [this mrs row]
    (conj! mrs row))

  (rs! [this mrs]
    (persistent! mrs)))

;;
;; Same as AggregateBuilder but truncates each row
;; on the fly using a tree of attributes (one pass).
;;

(deftype AggregateSelectBuilder
         [^ResultSet rs
          tree]

  jdbc.rs/RowBuilder

  (->row [this]
    (.getObject rs "aggregate"))

  (column-count [this]
    1)

  (with-column [this row i]
    row)

  (with-column-value [this row col v]
    row)

  (row! [this row]
    row)

  jdbc.rs/ResultSetBuilder

  (->rs [this]
    (transient []))

  (with-row [this mrs row]
    (conj! mrs (attrs/subnode row tree)))

  (rs! [this mrs]
    (persistent! mrs)))

(extend-protocol jdbc.rs/DatafiableRow

  PGobject

  (datafiable-row [this connectable opts]
    (case (.getType this)
      ("jsonb" "json")
      (-> this .getValue util/to-edn))))

(defn as-aggregates [^ResultSet rs opts]
  (new AggregateBuilder rs))

(defn ->as-aggregates
  "
  Having a list of attributes, produce a builder
  closed over a tree of aggregates for further
  truncating of rows.
  "
  [attrs]
  (let [tree (attrs/attrs->tree attrs)]
    (fn as-aggregates [^ResultSet rs opts]
      (new AggregateSelectBuilder rs tree))))
