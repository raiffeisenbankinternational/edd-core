(ns edd.view-store.postgres.honey
  "
  HoneySQL adapter for Next.JDBC.
  "
  (:refer-clojure :exclude [format])
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [edd.view-store.postgres.common :refer [error!]]
   [honey.sql :as sql]
   [honey.sql.pg-ops] ;; extend PG operators
   [lambda.util :as util]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]))

(alias 'cc 'clojure.core)

;;
;; *Warning*: when using maps with table namespaces like :users/email,
;; Postgres makes an extra query to fetch the tables related to
;; the columns. Thus, it doubles the amount of SELECT requests.
;;

(def jdbc-defaults
  {:builder-fn jdbc.rs/as-unqualified-maps})

(defn format
  ([sql-map]
   (sql/format sql-map nil))

  ([sql-map opt]
   (sql/format sql-map opt)))

;;
;; JDBC functions
;;

(defn execute
  "
  Format and execute a query by its HoneySQL representation.
  "
  ([db sql-map]
   (execute db sql-map nil))

  ([db sql-map opt]
   (let [[sql :as sql-vec] (format sql-map (:honey opt))]
     (util/d-time (cc/format "PG query: %s" sql)
                  (jdbc/execute! db
                                 sql-vec
                                 (merge jdbc-defaults opt))))))

(defn mask-params
  "
  Replace or mask some parameters before logging them.
  At the moment, omit aggregate maps (huge and insecure).
  "
  [params]
  (mapv (fn [param]
          (if (map? param)
            'SKIPPED
            param))
        params))

(defn execute-one
  "
  Like `execute` but return a single row.
  "
  ([db sql-map]
   (execute-one db sql-map nil))

  ([db sql-map opt]
   (let [[sql & params :as sqlvec]
         (format sql-map (:honey opt))

         message
         (if (seq params)
           (cc/format "PG query: %s, args: %s" sql (mask-params params))
           (cc/format "PG query: %s" sql))]

     (util/d-time message
                  (jdbc/execute-one! db
                                     sqlvec
                                     (merge jdbc-defaults opt))))))

(defn plan
  "
  Return a reducible object which processes the data
  on the fly while they come from network.
  "
  ([db sql-map]
   (plan db sql-map nil))

  ([db sql-map opt]
   (let [[sql :as sqlvec]
         (format sql-map (:honey opt))]

     (log/infof "PG plan: %s" sql)
     (jdbc/plan db
                sqlvec
                (merge jdbc-defaults opt)))))

;;
;; Helpers
;;

(defn insert [db table maps]
  (execute db {:insert-into table
               :values maps
               :returning [:*]}))

;;
;; SQL extensions
;;

(def BLANK (keyword ""))

(sql/register-op! :==)
(sql/register-op! BLANK)

(defn escape-like ^String [^String pattern]
  (let [escaped
        (-> pattern
            (str/trim)
            (.replace "%" "%%")
            (.replace "_" "%_"))]
    (str \% escaped \%)))

(defn ?-op
  "
  Build a HoneySQL expression which, when rendered,
  produces a string 'field1 ? field2'.
  "
  [field1 field2]
  [BLANK field1 [:raw "?"] field2])

(defn path-item->json [item]
  (cond

    (number? item)
    (-> item util/to-json)

    (keyword? item)
    (-> item str (subs 1) util/to-json)

    (string? item)
    (-> item str util/to-json)

    :else
    (error! "wrong json path item: %s" item)))

(defn ->inline-element [item]
  (cond

    (or (number? item)
        (string? item))
    [:inline item]

    (or (keyword? item)
        (symbol? item))
    [:inline (name item)]

    :else
    (error! "wrong json path item: %s" item)))

(defn ->array
  "
  Build an array HoneySQL expression.
  "
  [items]
  [:array (mapv ->inline-element items)])

(defn ilike
  "
  Build ilike HoneySQL expression. Escapes
  and inlines the string.
  "
  [field string]
  [:ilike field (escape-like string)])

(defn json-get-in
  "
  Build a HoneySQL structure that, when rendered,
  fetches a JSONb value from a field by a given path.
  "
  [field path]
  [:json#> field path])

(defn json-get-in-text
  "
  Like `json-get-in` but returns a text value
  from a JSONb field by a given path.
  "
  [field path]
  [:json#>> field path])

(defn unescape-??
  "
  When rendering, HoneySQL prepends ? with an extra ? to quote parameters
  (this is an agreement in JDBC). But it doens't work when passing a query
  into a COPY expression! For example:

  select data @?? '<json_path_expression>' -> works
  copy (select id where data @?? '<json_path_expression>') to ... -> broken syntax.

  There is a commit in HoneySQL that allows to override this behavior
  but it hasn't been released yet. For now, just replace @?? with @?

  See: https://github.com/seancorfield/honeysql/commit/b07ac78d68fb925ccbb52b2a2177a986cdf15308
  "
  [sql]
  (str/replace sql #"@\?\?" "@?"))

(sql/register-fn!
 :json->
 (fn [_ [field key]]
   (sql/format-expr [:nest [:-> field (->inline-element key)]])))

(sql/register-fn!
 :json->>
 (fn [_ [field key]]
   (sql/format-expr [:nest [:->> field (->inline-element key)]])))

(sql/register-fn!
 :json#>
 (fn [_ [field path]]
   (let [array (->array path)]
     (sql/format-expr [:nest [:#> field array]]))))

(sql/register-fn!
 :json#>>
 (fn [_ [field path]]
   (let [array (->array path)]
     (sql/format-expr [:nest [:#>> field array]]))))

(sql/register-fn!
 :like-regex
 (fn [_ [field value flags]]
   (let [node
         (cond-> [:raw
                  [:inline field]
                  " like_regex "
                  (util/to-json value)]
           flags
           (conj " flag " (-> flags name util/to-json)))]
     (sql/format-expr [:nest node]))))

nil ;; mute cider ouput when reloading this ns
