(ns lambda.ctx
  (:require [lambda.util :as util]
            [malli.core :as m]
            [malli.error :as me]))

(defn get-service-name
  "Resolves service name with fallback chain:
   1. :service-name already in ctx
   2. ResourceName / ServiceName env vars
   3. -Dedd.ServiceName system property
   Returns keyword or nil."
  [ctx]
  (or (:service-name ctx)
      (when-let [v (util/get-env "ResourceName")]
        (keyword v))
      (when-let [v (util/get-env "ServiceName")]
        (keyword v))
      (when-let [v (util/get-property "edd.ServiceName")]
        (keyword v))))

(defn get-hosted-zone-name
  "Resolves hosted zone name with fallback chain:
   1. :hosted-zone-name already in ctx
   2. PrivateHostedZoneName env var
   3. PublicHostedZoneName env var
   4. HostedZoneName env var
   Returns string or nil."
  [ctx]
  (or (:hosted-zone-name ctx)
      (util/get-env "PrivateHostedZoneName")
      (util/get-env "PublicHostedZoneName")
      (util/get-env "HostedZoneName")))

(def LambdaCtxSchema
  "Schema for lambda context configuration.
   Values are resolved from env vars, with ctx map as fallback."
  (m/schema
   [:map
    [:service-name
     {:description "Service identifier. Source: ResourceName env var, ServiceName env var, or :service-name in ctx"}
     keyword?]
    [:hosted-zone-name
     {:description "Hosted zone name. Source: PrivateHostedZoneName, PublicHostedZoneName, or HostedZoneName env var, or :hosted-zone-name in ctx"}
     [:string {:min 1}]]
    [:environment-name-lower
     {:description "Lowercase environment name used in resource naming (e.g. S3 buckets). Source: EnvironmentNameLower env var, or :environment-name-lower in ctx"}
     [:string {:min 1}]]]))

(defn init
  "Loads environment configuration into ctx:
    - secret.json config file
    - CustomConfig env var (JSON merged into ctx)
    - ResourceName, ServiceName, PrivateHostedZoneName/PublicHostedZoneName/HostedZoneName, EnvironmentNameLower env vars
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
          resolved (cond-> (assoc ctx
                                  :service-name
                                  (get-service-name ctx))
                     (and (not (contains? ctx :hosted-zone-name))
                          (get-hosted-zone-name ctx))
                     (assoc :hosted-zone-name (get-hosted-zone-name ctx))

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
