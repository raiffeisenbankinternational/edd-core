(ns edd.s3.view-store
  "S3-backed view store implementation for EDD-Core aggregate persistence.
   
   PREREQUISITES:
   - AWS credentials configured (via environment or IAM role)
   - S3 bucket: {AccountId}-{EnvironmentNameLower}-aggregates
   - Realm must be set in context [:meta :realm]
   
   STORAGE ARCHITECTURE:
   - Latest: s3://<bucket>/aggregates/<realm>/latest/<service>/<partition>/<id>.json
   - History: s3://<bucket>/aggregates/<realm>/history/<service>/<partition>/<id>/<version>.json
   - Partitioning: Last hex digit of aggregate ID (16-way partitioning)
   - Versioning: Full history preserved (all versions stored)
   
   IMPLEMENTATION NOTES:
   - Implements edd.search/update-aggregate, edd.search/get-snapshot, and edd.search/get-by-id-and-version multimethods
   - Stores both latest version (/latest path) AND historical versions (/history/{version} path)
   - Realm isolation enforced at path level
   - VERSION HISTORY: Fully supported - retrieves from /history/{version}.json
   - No optimistic locking - assumes ordered writes per aggregate
   
   USAGE:
   (-> ctx (s3-view-store/register))"
  (:require
   [sdk.aws.s3 :as s3]
   [lambda.util :as util]
   [edd.search :as search]
   [edd.search.validation :as validation]
   [clojure.tools.logging :as log]))

(defn ->realm
  [ctx]
  (or (some-> ctx :meta :realm name)
      (throw (new Exception "realm is not set"))))

(defn ->bucket-name
  [ctx]
  (let [account-id (get-in ctx [:aws :account-id])
        env-name (:environment-name-lower ctx)
        bucket (or (get-in ctx [:infra :storage :aggregates :bucket])
                   "aggregates")]
    (when-not account-id
      (throw (ex-info "account-id is not set in AWS context" {:aws (:aws ctx)})))
    (when-not env-name
      (throw (ex-info "environment-name-lower is not set in context" {:ctx-keys (keys ctx)})))
    (str account-id "-" env-name "-" bucket)))

(defn form-path
  [realm
   service-name
   id]
  (let [partition-prefix (-> (str id)
                             last
                             str
                             util/hex-str-to-bit-str)]
    (str "aggregates/"
         (name realm)
         "/latest/"
         (name service-name)
         "/"
         partition-prefix
         "/"
         id
         ".json")))

(defn form-history-path
  "Constructs S3 path for historical version of aggregate.
   Path: aggregates/{realm}/history/{service}/{partition}/{id}/{version}.json"
  [realm service-name id version]
  (let [partition-prefix (-> (str id)
                             last
                             str
                             util/hex-str-to-bit-str)]
    (str "aggregates/"
         (name realm)
         "/history/"
         (name service-name)
         "/"
         partition-prefix
         "/"
         id
         "/"
         version
         ".json")))

(defn store-to-s3
  [ctx aggregate]
  (let [id (str (:id aggregate))
        version (:version aggregate)
        realm (->realm ctx)
        bucket (->bucket-name ctx)

        ;; Latest path (existing)
        latest-key (form-path
                    realm
                    (:service-name ctx)
                    id)

        ;; History path (new)
        history-key (when version
                      (form-history-path realm (:service-name ctx) id version))

        payload (util/to-json {:aggregate aggregate
                               :service-name (:service-name ctx)
                               :realm realm})]

    ;; Write to /history/{version} first (if version present)
    ;; History is written before latest for transactional safety:
    ;; if we crash after latest but before history, latest would reference
    ;; a version that doesn't exist in history
    (when history-key
      (let [{:keys [error]} (s3/put-object ctx
                                           {:s3 {:bucket {:name bucket}
                                                 :object {:key history-key
                                                          :content payload}}})]
        (when error
          (throw (ex-info "Could not store aggregate to /history"
                          {:error error :version version})))))

    ;; Write to /latest (always)
    (let [{:keys [error]} (s3/put-object ctx
                                         {:s3 {:bucket {:name bucket}
                                               :object {:key latest-key
                                                        :content payload}}})]
      (when error
        (throw (ex-info "Could not store aggregate to /latest" {:error error}))))

    ctx))

(defn get-from-s3
  [{:keys [service-name] :as ctx} id]
  (let [id (str id)
        realm (->realm ctx)
        bucket (->bucket-name ctx)
        key (form-path
             realm
             service-name
             id)
        {:keys [error]
         :as resp} (s3/get-object ctx
                                  {:s3 {:bucket {:name bucket}
                                        :object {:key key}}})]
    (when (and error
               (not= (:status error)
                     404))
      (throw (ex-info "Could not store aggregate" {:error error})))
    (if (or
         (nil? resp)
         (= (:status error) 404))
      nil
      (-> resp
          slurp
          util/to-edn
          :aggregate))))

(defn get-history-from-s3
  "Retrieves specific version from S3 history path.
   Returns nil if version doesn't exist (404)."
  [{:keys [service-name] :as ctx} id version]
  (let [id (str id)
        realm (->realm ctx)
        bucket (->bucket-name ctx)
        key (form-history-path realm service-name id version)
        {:keys [error]
         :as resp} (s3/get-object ctx
                                  {:s3 {:bucket {:name bucket}
                                        :object {:key key}}})]
    (when (and error
               (not= (:status error) 404))
      (throw (ex-info "Could not retrieve historical aggregate"
                      {:error error :version version})))

    (if (or (nil? resp)
            (= (:status error) 404))
      nil
      (-> resp
          slurp
          util/to-edn
          :aggregate))))

;;; ============================================================================
;;; View Store Multimethod Implementations
;;; ============================================================================

(defmethod search/update-aggregate
  :s3
  [ctx aggregate]
  (validation/validate-aggregate! ctx aggregate)
  (let [id (:id aggregate)]
    (util/d-time
     (format "S3ViewStore update-aggregate, id: %s, version: %s" id (:version aggregate))
     (log/debug "Storing aggregate to S3" {:id id :version (:version aggregate)})
     (store-to-s3 ctx aggregate)
     ctx)))

(defmethod search/get-snapshot
  :s3
  [ctx id-or-query]
  (let [{:keys [id version]} (validation/validate-snapshot-query! ctx id-or-query)]
    (if version
      ;; Version history requested - retrieve from /history/{version}
      (util/d-time
       (format "S3ViewStore get-snapshot, id: %s, version: %s (history)" id version)
       (log/debug "Retrieving historical aggregate from S3" {:id id :version version})
       (get-history-from-s3 ctx id version))

      ;; Latest version - retrieve from /latest (unchanged)
      (util/d-time
       (format "S3ViewStore get-snapshot, id: %s (latest)" id)
       (log/debug "Retrieving latest aggregate from S3" {:id id})
       (get-from-s3 ctx id)))))

(defn register
  "Registers S3 as the view store implementation for this context.
   Usage: (-> ctx (s3-view-store/register))"
  [ctx]
  (assoc ctx :view-store :s3))

(defmethod search/get-by-id-and-version
  :s3
  [ctx id version]
  (validation/validate-id-and-version! ctx id version)
  ;; Delegate to get-snapshot
  (search/get-snapshot ctx {:id id :version version}))
