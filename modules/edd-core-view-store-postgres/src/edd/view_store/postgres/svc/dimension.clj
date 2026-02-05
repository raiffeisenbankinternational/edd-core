(ns edd.view-store.postgres.svc.dimension
  "
  Dimension service-specific queries.
  "
  (:require
   [edd.postgres.pool :refer [*DB*]]
   [edd.view-store.postgres.api :as api]
   [edd.view-store.postgres.common :refer [->realm]]
   [edd.view-store.postgres.const :as c]
   [edd.view-store.postgres.honey :as honey]))

(defn list-options-one-field
  "
  Backed logic for the `list-options-for-fields` query
  with one field.
  "
  [db realm field]
  (let [field-kw
        (keyword field)

        table
        (api/->table realm
                     c/SVC_DIMENSION
                     c/TAB_OPTIONS_1)

        sql-map
        {:select [:field_val]
         :from [table]
         :where [:= :field_key (name field)]}

        rows
        (honey/execute db sql-map)]

    {:options
     (for [{:keys [field_val]} rows]
       {field-kw field_val})}))

(defn list-options-two-fields
  "
  Backed logic for the `list-options-for-fields` query
  with two fields.
  "
  [db realm field1 field2]

  (let [field1-kw
        (keyword field1)

        field2-kw
        (keyword field2)

        table
        (api/->table realm
                     c/SVC_DIMENSION
                     c/TAB_OPTIONS_2)

        sql-map
        {:select [:field1_val
                  :field2_val]
         :from [table]
         :where [:and
                 [:= :field1_key (name field1)]
                 [:= :field2_key (name field2)]]}

        rows
        (honey/execute db sql-map)]

    {:options
     (for [{:keys [field1_val
                   field2_val]} rows]
       {field1-kw field1_val
        field2-kw field2_val})}))

(defn ctx-list-options-one-field [ctx field]
  (list-options-one-field *DB*
                          (->realm ctx)
                          field))

(defn ctx-list-options-two-fields [ctx field1 field2]
  (list-options-two-fields *DB*
                           (->realm ctx)
                           field1
                           field2))
