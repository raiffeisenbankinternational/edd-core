(ns edd.modules.import.query
  (:require [edd.core :as edd]
            [edd.common :as common]
            [batch.csv :as csv]
            [sdk.aws.s3 :as s3]))

(defn get-files
  [ctx {:keys [import]}]
  (let [files (:files import)
        bucket (:bucket import)]
    (reduce
     (fn [p [k v]]
       (assoc p
              k
              (let [is (s3/get-object ctx {:s3 {:bucket {:name bucket}
                                                :object {:key v}}})]
                (when (:error is)
                  (throw (ex-info "Error"
                                  {:error (:error is)})))
                (csv/parse-csv is \| true))))
     {}
     files)))

(defn register
  [ctx]
  (-> ctx
      (edd/reg-query :import->get-by-id common/get-by-id)
      (edd/reg-query :import->get-files get-files)))