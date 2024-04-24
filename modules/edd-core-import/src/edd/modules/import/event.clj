(ns edd.modules.import.event
  (:require [edd.core :as edd]
            [edd.modules.import.cmd :as cmd]))

(defn register
  [ctx]
  (-> ctx
      (edd/reg-event :import->import-started
                     (fn [agg _]
                       (assoc agg :import {})))
      (edd/reg-event :import->file-uploaded
                     (fn [agg event]
                       (-> agg
                           (update-in [:import :files]
                                      #(merge %
                                              (get-in event [:import :files])))
                           (assoc-in [:import :bucket]
                                     (get-in event [:import :bucket])))))
      (edd/reg-event :import->all-filed-uploaded
                     (fn [agg event]
                       (assoc-in agg
                                 [:import :status]
                                 (get-in event [:import :status]))))

      (edd/reg-event :import->all-filed-uploaded
                     (fn [agg event]
                       (assoc-in agg
                                 [:import :status]
                                 (get-in event [:import :status]))))
      (edd/reg-event cmd/upload-done-event
                     (fn [agg event]
                       (assoc-in agg
                                 [:import :status]
                                 (get-in event [:import :status]))))))
