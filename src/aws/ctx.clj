(ns aws.ctx
  (:import
   (java.net URL))
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
    [:aws-session-token {:optional true} string?]
    [:endpoint {:optional true}
     [:fn {:error/message "must be a proper URL starting with http(s)://..."}
      (fn [endpoint]
        (new URL endpoint))]]]))

(defn init
  [ctx]
  (let [endpoint
        (util/get-env "AWS_S3_ENDPOINT")

        region
        (or (util/get-env "Region")
            (util/get-env "AWS_DEFAULT_REGION" "local"))

        aws
        (cond-> {:region                region
                 :account-id            (util/get-env "AccountId" "local")
                 :aws-access-key-id     (util/get-env "AWS_ACCESS_KEY_ID" "")
                 :aws-secret-access-key (util/get-env "AWS_SECRET_ACCESS_KEY" "")
                 :aws-session-token     (util/get-env "AWS_SESSION_TOKEN" "")}

          endpoint
          (assoc :endpoint endpoint)

          :finally
          (merge (get ctx :aws)))]

    (if-not (m/validate AWSRuntimeSchema aws)
      (throw (ex-info "Error initializing aws config"
                      {:error (-> (m/explain AWSRuntimeSchema aws)
                                  (me/humanize))})))
    (assoc ctx :aws aws)))
