(ns lambda.ctx
  (:require [lambda.util :as util]
            [malli.core :as m]
            [malli.error :as me]))

(def LambdaCtxSchema
  "Schema for lambda context configuration.
   Values are resolved from env vars, with ctx map as fallback."
  (m/schema
   [:map
    [:service-name
     {:description "Service identifier. Source: ResourceName env var, ServiceName env var, or :service-name in ctx"}
     keyword?]
    [:hosted-zone-name
     {:description "Public hosted zone name. Source: PublicHostedZoneName env var, or :hosted-zone-name in ctx"}
     [:string {:min 1}]]
    [:environment-name-lower
     {:description "Lowercase environment name used in resource naming (e.g. S3 buckets). Source: EnvironmentNameLower env var, or :environment-name-lower in ctx"}
     [:string {:min 1}]]]))

(defn init
  "Loads environment configuration into ctx:
   - secret.json config file
   - CustomConfig env var (JSON merged into ctx)
    - ResourceName, ServiceName, PublicHostedZoneName, EnvironmentNameLower env vars
   Pre-existing ctx values take precedence over env vars. Env vars are
   used as fallback when a key is not already present in ctx.
   Validates resolved values against LambdaCtxSchema before returning.
   Idempotent: skips initialization if already done."
  [ctx]
  (if (:lambda-ctx-initialized ctx)
    ctx
    (let [ctx (-> ctx
                  (merge (util/load-config "secret.json"))
                  (merge (util/to-edn
                          (util/get-env "CustomConfig" "{}"))))
          resolved (cond-> ctx
                     (and (not (contains? ctx :service-name))
                          (util/get-env "ResourceName"))
                     (assoc :service-name (keyword (util/get-env "ResourceName")))

                     (and (not (contains? ctx :service-name))
                          (util/get-env "ServiceName"))
                     (assoc :service-name (keyword (util/get-env "ServiceName")))

                     (and (not (contains? ctx :hosted-zone-name))
                          (util/get-env "PublicHostedZoneName"))
                     (assoc :hosted-zone-name (util/get-env "PublicHostedZoneName"))

                     (and (not (contains? ctx :environment-name-lower))
                          (util/get-env "EnvironmentNameLower"))
                     (assoc :environment-name-lower (util/get-env "EnvironmentNameLower")))]
      (when-not (m/validate LambdaCtxSchema resolved)
        (throw (ex-info "Invalid lambda context configuration"
                        {:error (-> (m/explain LambdaCtxSchema resolved)
                                    (me/humanize))
                         :config (select-keys resolved [:service-name
                                                        :hosted-zone-name
                                                        :environment-name-lower])})))
      (assoc resolved :lambda-ctx-initialized true))))
