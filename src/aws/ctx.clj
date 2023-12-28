(ns aws.ctx
  (:require [lambda.util :as util]
            [malli.core :as m]
            [malli.error :as me]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def AWSRuntimeSchema
  (m/schema
   [:map
    [:region string?]
    [:account-id string?]
    [:aws-access-key-id string?]
    [:aws-secret-access-key string?]
    [:aws-session-token {:optional true} string?]]))

(defn init
  [ctx]
  (let [aws (merge {:region                (util/get-env "Region"
                                                         (util/get-env "AWS_DEFAULT_REGION" "local"))
                    :account-id            (util/get-env "AccountId" "local")
                    :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID" "")
                    :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY" "")
                    :aws-session-token     (util/get-env "AWS_SESSION_TOKEN" "")}
                   (get ctx :aws {}))]
    (if-not (m/validate AWSRuntimeSchema aws)
      (throw (ex-info "Error initializing aws config"
                      {:error (-> (m/explain AWSRuntimeSchema aws)
                                  (me/humanize))})))
    (assoc ctx :aws aws)))
