(ns edd.modules.import.fx
  (:require [edd.core :as edd]
            [edd.el.ctx :as el-ctx]))


(defn register
  [ctx]
  (-> ctx
      (edd/reg-event-fx :import->all-filed-uploaded
                        (fn [ctx _]
                          (let [agg (el-ctx/get-aggregate ctx)]
                            {:cmd-id :import->start-import
                             :import (:import agg)
                             :id     (:id agg)})))))
