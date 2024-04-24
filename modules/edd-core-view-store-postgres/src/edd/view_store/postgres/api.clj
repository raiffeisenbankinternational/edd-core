(ns edd.view-store.postgres.api
  "
  Basic CRUD operations for aggregates: create, upsert,
  bulk upsert, dump/copy, etc.
  "
  (:import
   clojure.lang.Keyword
   java.io.ByteArrayOutputStream
   java.io.InputStream
   java.io.OutputStream
   java.sql.Connection
   java.util.UUID
   org.postgresql.copy.CopyManager
   org.postgresql.core.BaseConnection)
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [edd.view-store.postgres.attrs :as attrs]
   [edd.view-store.postgres.common
    :refer [->realm
            ->service
            enumerate
            flatten-paths]]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.jdbc :refer [as-aggregates
                                         ->as-aggregates]]
   [edd.view-store.postgres.parser :as parser]
   [lambda.util :as util]
   [next.jdbc :as jdbc]))

(set! *warn-on-reflection* true)

(defn ->schema
  "
  Compose a database schema as a keyword.
  "
  ^Keyword [realm service]
  (keyword (format "%s-%s"
                   (name realm)
                   (name service))))

(defn ->table
  "
  Return a HoneySQL structure that, when rendering,
  becomes 'db_schema.table_name'.
  "
  ([schema]
   [[:. schema c/TABLE]])

  ([realm service]
   (->table (->schema realm service)))

  ([realm service table]
   [[:. (->schema realm service) table]]))

(defn ctx->table
  "
  Get a qualified table name (with a schema)
  out from the current context.
  "
  ([ctx]
   (->table (->realm ctx) (->service ctx) c/TABLE))

  ([ctx service]
   (->table (->realm ctx) service c/TABLE))

  ([ctx service table]
   (->table (->realm ctx) service table)))

(defn make-copy-manager
  "
  Build an instance of CopyManager from the current
  pool connection.
  "
  ^CopyManager [^Connection conn]
  (new CopyManager (.unwrap conn BaseConnection)))

(defmacro with-tmp-table
  "
  Run a block of code binding the `bind` symbol to the name
  of a temporary table. The `columns` vector is a set of
  pair like `[<name> <type>]`, e.g. `[:id :uuid]`.
  "
  [[db bind columns] & body]
  `(let [db# ~db
         bind# (gensym "tmp")
         ~bind bind#]
     (honey/execute db#
                    {:create-table [:temp bind#]
                     :with-columns ~columns})
     (try
       ~@body
       (finally
         (honey/execute db# {:drop-table bind#})))))

(defn rows->csv-input-stream
  "
  Turn a seq of rows into a CSV input stream.
  "
  ^InputStream [rows]
  (let [out (new ByteArrayOutputStream)]
    (with-open [writer (io/writer out)]
      (csv/write-csv writer rows :separator \,))
    (-> out
        .toByteArray
        io/input-stream)))

(defn get-by-id
  "
  Get a single aggregate by its UUID.
  "
  [db realm service aggregate-id]
  (let [table
        (->table realm service)

        sql-map
        {:select c/AGGREGATE_FIELDS
         :from [table]
         :where [:= :id [:inline aggregate-id]]}]

    (honey/execute-one db sql-map {:builder-fn as-aggregates})))

(defn get-by-ids
  "
  Fetch many aggregates by their UUIDs. Relies on the IN (?,?,...)
  expression and thus isn't meant to be used with vast id collections.
  "
  [db realm service aggregate-ids]

  (let [table
        (->table realm service)

        sql-map
        {:select c/AGGREGATE_FIELDS
         :from [table]
         :where [:in :id aggregate-ids]}]

    (honey/execute db sql-map {:builder-fn as-aggregates})))

(defn get-by-ids-copy
  "
  Fetch many aggregates by their UUIDs. Copies the ids
  into a temp table and INNER JOINs it to the target table.
  "
  [db realm service aggregate-ids]

  (with-open [conn (jdbc/get-connection db)]

    (with-tmp-table [conn tmp [[:id :uuid]]]

      (let [mgr
            (make-copy-manager conn)

            sql-copy
            (format "copy %s (id) from STDIN WITH (format CSV)"
                    (name tmp))

            table
            (->table realm service)

            sql-map
            {:select (for [field c/AGGREGATE_FIELDS]
                       [[:. table field]])
             :from [table]
             :join [[tmp] [:=
                           [:. table :id]
                           [:. tmp :id]]]}

            rows
            (for [aggregate-id aggregate-ids]
              [aggregate-id])

            input-stream
            (rows->csv-input-stream rows)]

        (.copyIn mgr sql-copy input-stream)

        (honey/execute conn sql-map {:builder-fn as-aggregates})))))

(defn upsert-many
  "
  Create or update many aggregates. Relies on the ON CONFLICT ...
  DO UPDATE SET ... Postgres expression. Bumps the `updated_at`
  column on update.

  Note: don't use when the number of aggregates is high. There is
  a COPY IN implementation for batch upsert.
  "
  [db realm service aggregates]
  (when (seq aggregates)

    (let [table
          (->table realm service)

          sql-map
          {:insert-into table
           :values
           (for [aggregate aggregates]
             {:id (:id aggregate)
              c/COL_AGGREGATE [:lift aggregate]})
           :on-conflict [:id]
           :do-update-set {c/COL_AGGREGATE :EXCLUDED.aggregate
                           :updated_at :current_timestamp}
           :returning c/AGGREGATE_FIELDS}]

      (honey/execute db sql-map {:builder-fn as-aggregates}))))

(defn upsert
  "
  Create or update a single aggregate.
  "
  [db realm service aggregate]
  (-> db
      (upsert-many realm service [aggregate])
      (first)))

(defn upsert-bulk
  "
  Upsert aggregates using the `jdbc/execute-batch!` bulk method.
  The default chunk size might be overridden.
  "
  ([db realm service aggregates]
   (upsert-bulk db realm service aggregates nil))

  ([db realm service aggregates {:keys [batch-size]}]
   (let [table
         (->table realm service)

         sql-map
         {:insert-into [table [:id :aggregate]]
          :values [[0 0]] ;; dummy placeholders
          :on-conflict [:id]
          :do-update-set {:aggregate :EXCLUDED.aggregate
                          :updated_at :current_timestamp}}

         [sql]
         (honey/format sql-map)

         rows
         (map (juxt :id identity) aggregates)

         opts
         {:batch-size batch-size}]

     (jdbc/with-transaction [tx db]
       (jdbc/execute-batch! tx sql rows opts)))))

(defn copy-in-csv
  "
  Having an input stream of CSV, insert aggregates in batch.
  The CSV must have two columns: the id and the JSON aggregate
  with NO headers.

  First, COPY IN the aggregates into a temp table. Then transfer
  them into the target table using the ON CONFLICT ... DO UPDATE SET
  expression. Bump the `updated_at` field for modified rows.

  The `src` argument is anything that can be transformed into
  a stream using the `io/input-stream` function: a file, another
  stream, a file path, etc.
  "
  ([db realm service src]
   (copy-in-csv db realm service src nil))

  ([db realm service src {:keys [header?]}]

   (with-open [conn (jdbc/get-connection db)]

     (let [mgr (make-copy-manager conn)]

       (with-tmp-table [conn tmp [[c/COL_ID :uuid]
                                  [c/COL_AGGREGATE :jsonb]]]

         (let [copy-in-opts
               (cond-> ["FORMAT CSV"]
                 header?
                 (conj "HEADER"))

               sql-copy
               (format "copy %s (id, aggregate) from STDIN WITH (%s)"
                       tmp
                       (->> copy-in-opts
                            (remove nil?)
                            (str/join ", ")))

               table
               (->table realm service)

               upsert-map
               {:insert-into [[table [:id c/COL_AGGREGATE]]
                              {:select [c/COL_ID c/COL_AGGREGATE]
                               :from tmp}]
                :on-conflict [:id]
                :do-update-set {c/COL_AGGREGATE :EXCLUDED.aggregate
                                :updated_at :current_timestamp}}]

           (with-open [stream (io/input-stream src)]
             (.copyIn mgr sql-copy stream))

           (honey/execute conn upsert-map)))))))

(defn copy-in
  "
  Having a seq of aggregates, upsert them using CSV + COPY IN.
  "
  [db realm service aggregates]

  (let [rows
        (for [{:as aggregate :keys [id]} aggregates]
          [id (util/to-json aggregate)])

        input-stream
        (rows->csv-input-stream rows)]

    (copy-in-csv db realm service input-stream)))

(defn delete-by-id
  "
  Delete a single aggregate by its UUID.
  "
  [db realm service aggregate-id]

  (let [table
        (->table realm service)

        sql-map
        {:delete-from table
         :where [:= :id aggregate-id]}]

    (honey/execute-one db sql-map)))

(defn find-aggregates
  "
  Find aggregates using an arbitrary HoneySQL `:where` clause.
  Supports limit, offest, and order-by HoneySQL expressions
  as well.

  The `attrs` is a seq of attrubute names to truncate each
  found aggregate.

  Avoid using this function directly. See the shortcuts
  build on top of it below.
  "
  ([db realm service where]
   (find-aggregates db realm service where nil))

  ([db realm service where {:keys [attrs
                                   limit
                                   offset
                                   order-by]}]

   (let [table
         (->table realm service)

         limit
         (or limit c/SIMPLE_SEARCH_LIMIT)

         offset
         (or offset 0)

         sql-map
         (cond-> {:select c/AGGREGATE_FIELDS
                  :from [table]
                  :limit [:inline limit]
                  :offset [:inline offset]}

           where
           (assoc :where where)

           order-by
           (assoc :order-by order-by))

         builder-fn
         (if (seq attrs)
           (->as-aggregates attrs)
           as-aggregates)]

     (honey/execute db sql-map {:builder-fn builder-fn}))))

(defn find-by-attrs
  "
  Find aggregates by a map like {attr => value}.

  Attributes might be nested, e.g. {:user {:id 1 :name 'john'}}.
  They will be flattened internally. All the attributes are unified
  with AND on SQL level.
  "
  ([db realm service attrs]
   (find-by-attrs db realm service attrs nil))

  ([db realm service attrs opt]
   (let [attrs-flat
         (flatten-paths attrs ".")

         where
         (->> attrs-flat
              (parser/attrs->filter)
              (parser/filter->where service))]

     (find-aggregates db realm service where opt))))

(defn find-advanced-parsed
  "
  Find aggregates using the **parsed** advanced search query.
  For unparsed query, see the function below.
  "
  [db realm service query-parsed]

  (let [{filter-parsed :filter
         search-parsed :search
         select-parsed :select
         sort-parsed :sort
         from-parsed :from
         size-parsed :size}
        query-parsed

        limit
        (parser/size-parsed->limit size-parsed)

        offset
        (parser/from-parsed->offset from-parsed)

        where-base
        (when filter-parsed
          (parser/filter-parsed->where service filter-parsed))

        order-by
        (when sort-parsed
          (parser/sort-parsed->order-by sort-parsed))]

    (if search-parsed

      ;;
      ;; TODO comment
      ;;

      (let [{:keys [attrs value]}
            search-parsed

            table
            (->table realm service)

            builder-fn
            (if (seq select-parsed)
              (->as-aggregates select-parsed)
              as-aggregates)

            sql-map
            {:select-distinct [:sub.aggregate]
             :from [[{:union
                      (for [[i attr] (enumerate attrs)]
                        (let [pred-wc
                              [:wildcard attr value]
                              where-wc
                              (parser/filter->where service pred-wc)]
                          {:select [:sub.order :sub.aggregate]
                           :from [[(cond-> {:select [[[:inline i] :order] :aggregate]
                                            :from [table]
                                            :where [:and where-base where-wc]
                                            :limit [:inline limit]
                                            :offset [:inline offset]}
                                     order-by
                                     (assoc :order-by order-by))
                                   :sub]]}))
                      :order-by [[:1 :asc]]}
                     :sub]]}]

        (honey/execute db sql-map {:builder-fn builder-fn}))

      ;;
      ;; If no search was passed, just find the aggregates
      ;; using the where expression obtained from `filter`.
      ;;
      (find-aggregates db
                       realm
                       service
                       where-base
                       {:attrs select-parsed
                        :limit limit
                        :offset offset
                        :order-by order-by}))))

(defn find-advanced
  "
  Find aggregates using the *unparsed* Open Search DSL query.
  "
  [db realm service query]
  (let [query-parsed
        (parser/parse-advanced-search! query)]
    (find-advanced-parsed db realm service query-parsed)))

(defn dump-aggregates
  "
  Dump aggregates in CSV format into the output.
  The `out` argument is anything that converts into
  the output stream using the `io/output-stream` function.
  The stream is closed internally.

  The output CSV payload as two columns: the ID and the JSON
  payload of an aggregate.

  Then `filter` parameter is an optional filter expression
  like `[:= :attrs.foo.bar 42]` which transformed into a SQL
  WHERE expression for additional filtering.

  Return the number of rows processed.
  "
  (^Long [db realm service out]
   (dump-aggregates db realm service out nil))

  (^Long [db realm service out {:keys [filter]}]

   (with-open [conn (jdbc/get-connection db)]

     (let [mgr
           (make-copy-manager conn)

           table
           (->table realm service)

           ^String sql-copy
           (if filter

             (let [where
                   (->> filter
                        (parser/parse-filter!)
                        (parser/filter-parsed->where service))

                   sql-map
                   {:select [:id c/COL_AGGREGATE]
                    :from [table]
                    :where where}

                   [sql-query]
                   (honey/format sql-map)]

               (-> "copy (%s) TO STDOUT WITH (format csv)"
                   (format sql-query)
                   (honey/unescape-??)))

             (let [sql-table
                   (-> table
                       (first)
                       (honey/format)
                       (first))]
               (format "copy %s (id, aggregate) TO STDOUT WITH (format csv)" sql-table)))]

       (with-open [stream (io/output-stream out)]
         (.copyOut mgr sql-copy stream))))))

(defn read-aggregates-dump
  "
  Read aggregates from an output produced by the `dump-aggregates`
  function. The `src` argument is anything that gets converted
  into an input stream using the `io/input-stream` function.

  The optional `attrs` parameter is a seq of attributes to truncate
  each aggregate when reading.

  Return a vector of aggregate maps.
  "
  ([src]
   (read-aggregates-dump src nil))

  ([src attrs]
   (let [tree
         (some-> attrs attrs/attrs->tree)

         tx-base
         (comp
          (map second)
          (map util/to-edn)
          (remove :__generated__)) ;; skip dev generated rows

         tx-final
         (cond-> tx-base
           tree
           (comp (map
                  (fn [row]
                    (attrs/subnode row tree)))))]

     (with-open [in (io/reader src)]
       (into [] tx-final (csv/read-csv in))))))
