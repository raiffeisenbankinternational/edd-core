(ns edd.postgres.const
  "
  Constant values.
  ")

(def AGGREGATE_HISTORY_FIELDS
  [:id :version :aggregate :service-name :valid-from :valid-until])

(def HISTORY-TABLE :aggregates-history)
