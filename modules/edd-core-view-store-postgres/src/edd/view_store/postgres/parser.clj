(ns edd.view-store.postgres.parser
  "
  DSL parser and transformer. Mostly used to transcode
  Open Search DSL into HoneySQL.
  "
  (:import
   clojure.lang.Keyword
   java.time.LocalDate
   java.time.LocalTime
   java.time.OffsetDateTime
   java.time.ZoneOffset)
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [edd.view-store.postgres.attrs :as attrs]
   [edd.view-store.postgres.common :refer [->long!
                                           error!]]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.schema :as schema]
   [honey.sql :as sql]
   [honey.sql.pg-ops :refer [at?
                             atat
                             tilde*]]
   [lambda.uuid :as uuid]
   [lambda.util :as util]
   [malli.core :as malli]))

(defn parse-time-min ^OffsetDateTime [^String yyyy-mm-dd]
  (-> yyyy-mm-dd
      LocalDate/parse
      (OffsetDateTime/of LocalTime/MIN ZoneOffset/UTC)))

(defn parse-time-max ^OffsetDateTime [^String yyyy-mm-dd]
  (-> yyyy-mm-dd
      LocalDate/parse
      (OffsetDateTime/of LocalTime/MAX ZoneOffset/UTC)))

(defn malli-parse! [data Parser error-message]
  (let [result (Parser data)]
    (if (= result ::malli/invalid)
      (throw (ex-info error-message
                      {:message error-message
                       :data data}))
      result)))

(defn malli-validate! [data Schema error-message]
  (if (malli/validate Schema data)
    data
    (throw (ex-info error-message
                    {:data data}))))

(defn parse-advanced-search! [data]
  (malli-parse! data
                schema/AdvancedSearchParser
                "could not parse the advanced search DSL"))

(defn parse-filter! [data]
  (malli-parse! data
                schema/FilterParser
                "could not parse OS filter DSL"))

(defn parse-sort! [data]
  (malli-parse! data
                schema/SortParser
                "could not parse OS sort DSL"))

(defn validate-simple-search! [data]
  (malli-validate! data
                   schema/SimpleSearch
                   "simple search query is invalid"))

(defn needs-quoting? ^Boolean [^String attr]
  (some? (re-find #"[^a-zA-Z0-9_]" attr)))

(defn ->maybe-quote [^Keyword item]
  (if (-> item name needs-quoting?)
    (name item)
    item))

(defn path->json-attr
  "
  Turn an attribute path (a vector of keywords) into
  a HoneySQL structure that becomes \"foo\".\"bar\"
  when rendering. Accepts the lead symbol for JSON
  path, e.g. $ or @.
  "
  [lead path]
  (reduce
   (fn [acc item]
     [:. acc (->maybe-quote item)])
   [:raw lead]
   path))

(def path->root-attr
  (partial path->json-attr "$"))

(def path->this-attr
  (partial path->json-attr "@"))

(defn order-parsed->order [order-parsed]
  (case (name order-parsed)
    ("asc" "asc-number" "asc-date") :asc
    ("desc" "desc-number" "desc-date") :desc))

(defn sort-parsed->order-by
  "
  Turn the parsed :sort structure into a HoneySQL
  :order-by structure.
  "
  [service sort-parsed]
  (vec
   (for [{:keys [attr order]}
         sort-parsed]

     (let [sql-order
           (order-parsed->order order)

           path
           (-> attr
               attrs/attr->path)

           sort-field
           (case path

             ;; common sorting cases
             ([:creation-time]
              [:attrs :creation-time])
             :created_at

             ;; service-dependent cases
             (case [service path]

               ;; A special case for the application ans task-manager
               ;; services: when sorting by application-id, it must be
               ;; coerced to integer for proper ordering. Both services
               ;; have database indexes on these expressions.
               ([:glms-application-svc [:attrs :application-id]]
                [:glms-task-manager-svc [:attrs :application :application-id]])
               [:cast (honey/json-get-in-text c/COL_AGGREGATE path) :int]

                ;; fallback
               (honey/json-get-in-text c/COL_AGGREGATE path)))]

       [sort-field sql-order]))))

(defn ->json-op [op]
  (case (name op)
    ("eq" "=" "==") :==
    ("lt" "<")   :<
    ("lte" "<=") :<=
    ("gte" ">=") :>=
    ("gt" ">")   :>))

(defn ->sql-op [op]
  (case (name op)
    ("eq" "=" "==") :=
    ("lt" "<")   :<
    ("lte" "<=") :<=
    ("gte" ">=") :>=
    ("gt" ">")   :>))

(defn ->json-cond [condition]
  (case (name condition)
    "and" :&&
    "or" :||))

(defn ->string
  "
  Turn a value into a string for IN expression.
  Sometimes, frontend or other services pass integers.
  "
  ^String [value]
  (cond

    (string? value)
    value

    (uuid? value)
    (str \# value)

    (nil? value)
    nil

    :else
    (str value)))

(defn filter-parsed->where
  "
  Having a service name and a parsed `:filter` expression,
  compose a `:where` HoneySQL data structure. When rendered,
  it becomes the WHERE SQL expression.

  For parameters, we use [:inline ...] tags to see the actual
  values in logs.

  See the schema of the OpenSearch DSL for more details.
  "
  [service filter-parsed]

  (let [[tag content]
        filter-parsed]

    (case tag

      :negation
      (let [{:keys [query]}
            content]
        [:not (filter-parsed->where service query)])

      ;; just eliminate such a group
      :group-broken
      nil

      (:group-variadic :group-array)
      (let [{:keys [condition
                    children]}
            content]

        (-> [condition]
            (into (mapv filter-parsed->where (repeat service) children))))

      :predicate-simple
      (let [{:keys [op attr value]}
            content

            attr-name
            (name attr)

            path
            (-> attr
                attrs/attr->path)

            json-op
            (->json-op op)

            field
            (-> path
                path->root-attr)

            [field-str]
            (sql/format field)

            json-path
            [json-op [:raw field-str] [:raw (util/to-json value)]]

            [json-path-str]
            (sql/format-expr json-path)

            btree?
            (attrs/path-btree? service path)]

        ;; Here and below: if it's a btree attribute, we generate
        ;; a btree expression that hits its own index. Otherwise,
        ;; generate a JSONpath with a predicate expression.

        (if btree?
          [(->sql-op op)
           (honey/json-get-in-text c/COL_AGGREGATE path)
           [:inline (->string value)]]
          [atat c/COL_AGGREGATE [:inline json-path-str]]))

      :predicate-in
      (let [{:keys [attr value]}
            content

            field
            (-> attr
                attrs/attr->path
                path->root-attr)

            amount
            (count value)]

        (cond

          ;; Problem: people keep sending broken predicates like
          ;; [:in attrs.user.id []] (empty IN clause). To prevent
          ;; failures on production, blindly return FALSE so the
          ;; entire branch gets resolved into false (mimics Open
          ;; Search behavior).
          (= 0 amount)
          false

          (= 1 amount)
          (recur service
                 [:predicate-simple
                  {:op :=
                   :attr attr
                   :value (first value)}])

          :else
          (let [json-path
                (into [:||]
                      (for [v value]
                        [:== [:raw "@"] [:raw (util/to-json v)]]))

                path
                (attrs/attr->path attr)

                [field-str]
                (sql/format field)

                json-path
                (honey/?-op [:raw field-str] json-path)

                [json-path-str]
                (sql/format json-path)

                btree?
                (attrs/path-btree? service path)

                path
                (attrs/attr->path attr)]

            (if btree?
              [:in
               [:json#>> c/COL_AGGREGATE path]
               (for [v value]
                 [:inline (->string v)])]
              [at? c/COL_AGGREGATE [:inline json-path-str]]))))

      ;;
      ;; For this field, frontend passes integers, but we have
      ;; strings in the database. Coerce to strings.
      ;;
      :predicate-asset-class-code
      (let [{:keys [attr
                    value]}
            content]
        (recur service
               [:predicate-in
                {:op :in
                 :attr attr
                 :value (map str value)}]))

      ;;
      ;; Here and below: the datetime cases need some adjustment.
      ;; In JSON, we store full ISO strings like YYYY-mm-ddTHH:MM:SS.xxxZ.
      ;; But the frontend passes only YYYY-mm-dd. Thus, depending
      ;; on the operator, we add either trailing 'allballs' (00:00:00)
      ;; or '23:59:59'.
      ;;
      :predicate-datetime-less
      (let [{:keys [op
                    attr
                    value]}
            content]
        (recur service
               [:predicate-simple
                {:op op
                 :attr attr
                 :value (-> value parse-time-max util/date->string)}]))

      :predicate-datetime-more
      (let [{:keys [op
                    attr
                    value]}
            content]
        (recur service
               [:predicate-simple
                {:op op
                 :attr attr
                 :value (-> value parse-time-min util/date->string)}]))

      ;;
      ;; Correct the expression such that it hits the primary key
      ;; index but not the JSONpath index.
      ;;
      :predicate-id-uuid
      (let [{:keys [value]}
            content]
        [:= :id [:inline value]])

      :predicate-in-uuid
      (let [{:keys [attr value]}
            content

            len
            (count value)]

        (cond

          (= len 1)
          [:= :id [:inline (first value)]]

          (> len 1)
          [:in :id (for [uuid value]
                     [:inline uuid])]

          :else
          (error! "Empty IN predicate, attribute: %s" attr)))

      ;;
      ;; There is *someone* who passes an expression like
      ;; [:in :attr.foo :this :that] which is incorrect,
      ;; but I cannot spot it.
      ;;
      :predicate-status-variadic
      (let [{:keys [attr
                    value1
                    value2]}
            content]
        (recur service
               [:predicate-in
                {:op :in
                 :attr attr
                 :value [value1 value2]}]))

      :predicate-wc
      (let [{:keys [attr
                    value]}
            content

            path
            (attrs/attr->path attr)

            field
            (honey/json-get-in-text c/COL_AGGREGATE path)

            wildcard?
            (attrs/path-wildcard? service path)]

        (cond

          ;;
          ;; Custom cases for the dimension service
          ;; plus <cocunut>/<short-name> input
          ;;
          (attrs/dimension-cocunut? service attr)
          (let [cocunut (attrs/term->cocunut value)]
            (honey/ilike field cocunut))

          (attrs/dimension-short-name? service attr)
          (let [short-name (attrs/term->short-name value)]
            (honey/ilike field short-name))

          ;; Try to hit the wildcard index
          wildcard?
          (honey/ilike field value)

          ;; Otherwise, produce the jsonpath expression with the regex_like
          ;; operator. This operator doens't support the GIN/jsonb_path_ops
          ;; index and thus is slow (the last resort).
          :else
          (let [field
                (path->root-attr path)

                json-path
                [:like-regex field value :iq]

                [json-path-str]
                (sql/format json-path)]

            [atat c/COL_AGGREGATE [:inline json-path-str]])))

      :predicate-exists
      (let [{:keys [attr]}
            content

            field
            (-> attr
                attrs/attr->path
                path->root-attr)

            [json-path-str]
            (sql/format field)]

        [at? c/COL_AGGREGATE [:inline json-path-str]])

      :predicate
      (recur service content)

      :nested
      (let [{:keys [attr
                    group]}
            content

            attr-path
            (attrs/attr->path attr)

            to-drop
            (count attr-path)

            {:keys [condition
                    children]}
            group

            field
            (path->root-attr attr-path)

            op-top
            (->json-cond condition)

            sub-condition
            (for [[tag predicate-parsed]
                  children]

              (case tag

                :predicate-simple
                (let [{:keys [op attr value]}
                      predicate-parsed

                      field
                      (->> attr
                           (attrs/attr->path)
                           (drop to-drop)
                           (path->this-attr))

                      json-op
                      (->json-op op)]

                  [json-op field [:raw (util/to-json value)]])

                ;;
                ;; Nested wildcard expressions always use like_regex
                ;;
                :predicate-wc
                (let [{:keys [attr value]}
                      predicate-parsed

                      field
                      (->> attr
                           (attrs/attr->path)
                           (drop to-drop)
                           (path->this-attr))]

                  [:like-regex field value :iq])))

            sub-joined
            (into [op-top] sub-condition)

            json-path
            (honey/?-op field sub-joined)

            [json-path-str]
            (sql/format json-path)]

        [at? c/COL_AGGREGATE [:inline json-path-str]]))))

(defn filter->where
  "
  Turn a filter expression into a HoneySQL :where
  data strucure.
  "
  [service filter]
  (some->> filter
           (parse-filter!)
           (filter-parsed->where service)))

(defn from-parsed->offset
  "
  Take a parsed :from clause and obtain the final value
  from it as an integer.
  "
  [from-parsed]
  (let [[from-tag from-value]
        from-parsed]
    (case from-tag
      :integer from-value
      :string (->long! from-value)
      nil 0)))

(defn size-parsed->limit
  "
  Take a parsed :size clause and obtain the final value
  from it as an integer.
  "
  [size-parsed]
  (let [[size-tag size-value]
        size-parsed]
    (case size-tag
      :integer size-value
      :string (->long! size-value)
      nil c/ADVANCED_SEARCH_LIMIT)))

(defn sort->order-by
  "
  Turn the unparsed `sort` expression into the :order-by
  HoneySQL data structure.
  "
  [service os-sort]
  (->> os-sort
       (parse-sort!)
       (sort-parsed->order-by service)))

(defn attrs->filter
  "
  Turn a map of {attr -> value} into filter expression
  like [:and [:= attrs.foo 1] [:= attrs.bar 2]]
  "
  [attrs]
  (reduce-kv
   (fn [acc attr value]
     (let [op
           (if (or (sequential? value)
                   (set? value))
             :in
             :eq)]
       (conj acc [op attr value])))
   [:and]
   attrs))
