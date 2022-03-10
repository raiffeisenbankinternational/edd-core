(ns edd.modules.import.cmd
  (:require [clojure.tools.logging :as log]
            [edd.core :as edd]
            [clojure.string :as str]
            [lambda.uuid :as uuid]))

(defn object-upload-handler
  [{:keys [progress] :as ctx} {:keys [key bucket]}]
  (let [files (get-in ctx [:module :import :files])
        uploaded-files (get-in progress [:import :files])
        current-file (first
                      (filter
                       #(str/ends-with? key (str
                                             (name %)
                                             ".csv"))
                       files))
        new-state (assoc uploaded-files current-file key)]
    (log/info "Uploaded" current-file)
    (log/info "Current state: " (sort (keys new-state)))
    (log/info "Expected state: " (sort files))
    (cond
      (= (sort (keys new-state))
         (sort files)) (do
                         [{:event-id :import->file-uploaded
                           :import   {:bucket bucket
                                      :files  {current-file key}}}
                          {:event-id :import->all-filed-uploaded
                           :import   {:status :uploaded}}])
      current-file {:event-id :import->file-uploaded
                    :import   {:bucket bucket
                               :files  {current-file key}}}
      :else (do
              (log/warn "Ignoring: " key)
              {}))))

(def upload-done-event :import->upload-ready)

(defn start-import
  [_ctx _cmd]
  {:event-id upload-done-event
   :import {:status :done}})

(defn register
  [ctx]
  (-> ctx
      (edd/reg-cmd :object-uploaded object-upload-handler
                   :id-fn (fn [_ctx cmd]
                            (uuid/named (:date cmd)))
                   :consumes [:map
                              [:bucket :string]
                              [:key :string]]
                   :deps {:progress (fn [_ cmd]
                                      {:id       (uuid/named (:date cmd))
                                       :query-id :import->get-by-id})})
      (edd/reg-cmd :import->start-import start-import
                   :deps {:files (fn [_ cmd]
                                   {:id       (:id cmd)
                                    :import   (:import cmd)
                                    :query-id :import->get-files})})))
