(ns edd.response.s3
  (:require [sdk.aws.s3 :as s3]
            [lambda.util :as util]
            [edd.response.cache :refer :all]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defmethod cache-response
  :s3
  [{:keys [aws service-name] :as ctx} {:keys [idx] :as resp}]
  (util/d-time
   (str "Storing cache response: " idx)
   (let [s3 {:s3 {:bucket {:name (str (:account-id aws)
                                      "-"
                                      (:environment-name-lower ctx)
                                      "-sqs")}
                  :object {:key (str "response/"
                                     (:request-id ctx)
                                     "/"
                                     (str/join "-" (:breadcrumbs ctx))
                                     "/"
                                     (name service-name)
                                     (when idx
                                       (str "-part." idx))
                                     ".json")}}}
         {:keys [error]} (s3/put-object
                          ctx
                          (assoc-in s3
                                    [:s3 :object :content]
                                    (util/to-json {:resp    resp
                                                   :service service-name})))]
     (if error
       {:error error}
       s3))))

(defn register
  [ctx]
  (update ctx :response-cache #(or % :s3)))
