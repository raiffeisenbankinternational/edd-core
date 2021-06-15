(ns edd.response.s3
  (:require [sdk.aws.s3 :as s3]
            [lambda.util :as util]
            [edd.response.cache :refer :all]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defmethod cache-response
  :s3
  [{:keys [resp aws service-name] :as ctx}]
  (log/info "Storing cache response")
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
                                    ".json")}}}
        {:keys [error]} (s3/put-object
                         (assoc-in s3
                                   [:s3 :object :content]
                                   (util/to-json {:resp    resp
                                                  :service service-name})))]
    (if error
      {:error error}
      s3)))

(defn register
  [ctx]
  (if-not (:response-cache ctx)
    (assoc ctx :response-cache :s3)
    ctx))