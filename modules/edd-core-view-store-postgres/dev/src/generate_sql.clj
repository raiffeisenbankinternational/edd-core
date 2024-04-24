(ns generate-sql
  "
  Generate tables and indexes for dev setup.
  "
  (:import
   java.io.File)
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [lambda.util :as util]
   [edd.view-store.postgres.attrs :as attrs]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]
   [edd.view-store.postgres.parser :as parser]))

(defn ->sql [item]
  (-> item
      name
      (str/replace #"-" "_")
      (str/replace #"\." "_")))

(defn ->curly [item]
  (format "{%s}" item))

(defn ->quote [item]
  (format "'%s'" item))

(defn ->schema [realm service]
  (format "%s_%s"
          (name realm)
          (->sql service)))

(defn drop-keyword [item]
  (str/replace (name item) #"\.keyword" ""))

(defn ->title [headline]
  (with-out-str
    (println)
    (println "--------")
    (println "--" (name headline))
    (println "--------")))

(def TEMPLATE_CREATE_TABLE "

create table if not exists %s.aggregates (
    id UUID primary key,
    aggregate JSONB COMPRESSION lz4 not null,
    created_at timestamp with time zone not null default current_timestamp,
    updated_at timestamp with time zone null
);

")

(def TEMPLATE_IDX_JSON_PATH "

create index if not exists idx_aggregates_aggregate_gin_jsonb_path on %s.aggregates using gin (aggregate jsonb_path_ops);

")

(def TEMPLATE_IDX_CREATED_AT "

create index if not exists idx_aggregates_created_at_desc on %s.aggregates (created_at desc);

")

(def TEMPLATE_IDX_CREATED_AT_DROP "

drop index if exists %s.idx_created_at;

")

(def TEMPLATE_ADD_WC_INDEX "

create index if not exists idx_aggregates_%s_trgm on %s.aggregates using gist ((%s) gist_trgm_ops);

")

(def TEMPLATE_ADD_WC_INDEX_DIMENSION_SLASH "

create index if not exists idx_aggregates_attrs_cocunut_slash_short_name_trgm on %s.aggregates using gist (((aggregate #>> '{attrs,cocunut}') || '/' || (aggregate #>> '{attrs,short-name}')) gist_trgm_ops);

")

(def TEMPLATE_DROP_WC_INDEX "

drop index if exists %s.idx_aggregates_%s_trgm;

")

(def TEMPLATE_ADD_BTREE_INDEX "

create index idx_aggregates_%s_btree on %s.aggregates using btree ((%s));

")

(def TEMPLATE_DROP_BTREE_INDEX "

drop index if exists %s.idx_aggregates_%s_btree;

")

(defn frmt [string & args]
  (str/trim (apply format string args)))

(defn generate-tables [realm]

  (println)

  (doseq [service c/SERVICES]

    (println (->title service))

    (let [schema
          (->schema realm service)]

      (println (frmt TEMPLATE_CREATE_TABLE schema))
      (println (frmt TEMPLATE_IDX_JSON_PATH schema))
      (println (frmt TEMPLATE_IDX_CREATED_AT schema))

      nil))

  (println))

(defn ->array [attr]
  (->> (str/split (name attr) #"\.")
       (str/join ",")
       (->curly)
       (->quote)))

(defn generate-wc-indexes [realm]

  (println)

  (doseq [[service paths]
          attrs/PATHS_WC]

    (println (->title service))

    (doseq [path paths]

      (let [attr
            (attrs/path->attr path)

            schema
            (->schema realm service)

            column
            (->sql attr)

            array
            (->array attr)

            expression
            (format "aggregate #>> %s" array)]

        (println (frmt TEMPLATE_DROP_WC_INDEX schema column))
        (println (frmt TEMPLATE_ADD_WC_INDEX column schema expression))
        (println)))))

(defn generate-btree-indexes [realm]

  (println)

  (doseq [[service paths]
          attrs/PATHS_BTREE]

    (println (->title service))

    (doseq [path paths]

      (let [attr
            (attrs/path->attr path)

            schema
            (->schema realm service)

            column
            (->sql attr)

            array
            (->array attr)

            expression
            (format "aggregate #>> %s" array)]

        (println (frmt TEMPLATE_DROP_BTREE_INDEX schema column))
        (println (frmt TEMPLATE_ADD_BTREE_INDEX column schema expression))
        (println)))))

(def TEMPLATE_PREWARM "

with indexes (index) as (values
%s
)
select
	pg_prewarm(index) as blocks, index
from
    indexes
order by
    blocks desc;

")

(defn generate-prewarm-query [realm]
  (let [expressions
        (flatten
         (for [service c/SERVICES]

           (let [schema
                 (->schema realm service)

                 paths-wc
                 (get attrs/PATHS_WC service)

                 paths-btree
                 (get attrs/PATHS_BTREE service)]

             [(format "    ('%s.idx_aggregates_aggregate_gin_jsonb_path')" schema)
              (format "    ('%s.idx_aggregates_created_at_desc')" schema)
              (for [path paths-wc]
                (format "    ('%s.idx_aggregates_%s_trgm')" schema (-> path attrs/path->attr ->sql)))
              (for [path paths-btree]
                (format "    ('%s.idx_aggregates_%s_btree')" schema (-> path attrs/path->attr ->sql)))])))]

    (println (frmt TEMPLATE_PREWARM (str/join ",\n" expressions)))))

(defn generate-dimension-slash-wc-index [realm]
  (let [schema (->schema realm :glms-dimension-svc)]
    (println (frmt TEMPLATE_ADD_WC_INDEX_DIMENSION_SLASH schema))))

(defn get-last-migration-id ^Long [^File path]
  (->> path
       (file-seq)
       (map (fn [^File file]
              (->> file (.getName) (re-find #"^V(\d+)__") second)))
       (remove nil?)
       (map parse-long)
       (sort)
       (last)))

(defn get-next-migration-id ^Long [^File path]
  (let [mig-id (get-last-migration-id path)]
    (if mig-id
      (inc mig-id)
      1)))

(defn service->mig-dir ^File [service]
  (io/file (format "../../../%s/ansible/deploy/edd-core-view-store-postgres/migrations"
                   (name service))))

(defn make-mig-filename ^String [mig-id slug]
  (format "V%03d__%s.sql" mig-id slug))

(defn generate-migration-btree [slug]

  (let [files (new java.util.ArrayList)]

    (doseq [[service paths] attrs/PATHS_BTREE]

      (let [dir
            (service->mig-dir service)

            mig-id
            (get-next-migration-id dir)

            filename
            (make-mig-filename mig-id slug)

            file
            (io/file dir filename)]

        (.mkdirs dir)

        (binding [*out* (io/writer file)]

          (doseq [path paths]

            (let [attr
                  (attrs/path->attr path)

                  column
                  (->sql attr)

                  array
                  (->array attr)

                  expression
                  (format "aggregate #>> %s" array)]

              (println (format "create index if not exists idx_aggregates_%s_btree on aggregates\nusing btree ((%s));"
                               column expression))
              (println)))

          (.add files file))))

    (vec files)))

(defn generate-migration-wildcard [slug]

  (let [files (new java.util.ArrayList)]

    (doseq [[service paths] attrs/PATHS_WC]

      (let [dir
            (service->mig-dir service)

            mig-id
            (get-next-migration-id dir)

            filename
            (make-mig-filename mig-id slug)

            file
            (io/file dir filename)]

        (.mkdirs dir)

        (binding [*out* (io/writer file)]

          (doseq [path paths]

            (let [attr
                  (attrs/path->attr path)

                  column
                  (->sql attr)

                  array
                  (->array attr)

                  expression
                  (format "aggregate #>> %s" array)]

              (println (format "create index if not exists idx_aggregates_%s_trgm on aggregates\nusing gist ((%s) gist_trgm_ops);"
                               column expression))
              (println)))

          (.add files file))))

    (vec files)))

(comment

  (defn generate-wc-mapping []
    (let [entries
          (-> "/Users/ivan.grishaev-external/Downloads/logs-insights-results.json"
              io/file
              util/to-edn)

          tree
          (reduce
           (fn [acc {:keys [attr
                            service]}]
             (assoc-in acc [(keyword service) (drop-keyword attr)] nil))
           {}
           entries)]

      (update-vals tree (comp set keys))))

  nil)
