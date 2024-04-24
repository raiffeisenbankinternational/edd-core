(ns edd.s3.view-store
  "
  The S3 view store common utilities. Moved here from
  edd.elastic.view-store to be used withing other stores.
  "
  (:require
   [sdk.aws.s3 :as s3]
   [lambda.util :as util]))

(defn ->realm
  [ctx]
  (or (some-> ctx :meta :realm name)
      (throw (new Exception "realm is not set"))))

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

(defn store-to-s3
  [{:keys [aggregate
           service-name] :as ctx}]
  (let [id (str (:id aggregate))

        realm (->realm ctx)
        bucket (str
                (util/get-env
                 "AccountId")
                "-"
                (util/get-env
                 "EnvironmentNameLower")
                "-aggregates")
        key (form-path
             realm
             service-name
             id)
        {:keys [error]
         :as resp} (s3/put-object ctx
                                  {:s3 {:bucket {:name bucket}
                                        :object {:key key
                                                 :content (util/to-json {:aggregate aggregate
                                                                         :service-name service-name
                                                                         :realm realm})}}})]
    (when error
      (throw (ex-info "Could not store aggregate" {:error error})))
    resp))

(defn get-from-s3
  [{:keys [service-name] :as ctx} id]
  (let [id (str id)
        realm (->realm ctx)
        bucket (str
                (util/get-env
                 "AccountId")
                "-"
                (util/get-env
                 "EnvironmentNameLower")
                "-aggregates")
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
