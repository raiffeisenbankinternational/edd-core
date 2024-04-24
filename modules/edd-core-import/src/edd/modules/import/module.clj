(ns edd.modules.import.module
  (:require [edd.modules.import.cmd :as cmd]
            [edd.modules.import.query :as query]
            [edd.modules.import.event :as event]
            [edd.modules.import.fx :as fx]
            [edd.postgres.event-store :as postgres-event-store]
            [edd.core :as edd]))

(def upload-done-event cmd/upload-done-event)

(defn import-handler
  [ctx body]
  (with-redefs [postgres-event-store/store-effects
                (fn [_ctx _cmd]
                  ; Do nothing. We rely on events being stored in S3 bucket
                  )]
    (edd/handler ctx body)))

(defn register
  [ctx & {:keys [files]}]
  (-> ctx
      (assoc-in [:module :import :files] files)
      (cmd/register)
      (query/register)
      (fx/register)
      (event/register)))
