(ns lambda.jwt
  (:require [lambda.util :as util]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io])

  (:import (com.auth0.jwt.algorithms Algorithm)
           (com.auth0.jwt JWT)
           (com.auth0.jwk Jwk)
           (com.auth0.jwt.exceptions SignatureVerificationException)))

(defn validate-token-attributes
  [{{region :region}                                  :env
    {user-pool-id :user-pool-id client-id :client-id} :auth} token]
  (-> {:jwk       :valid
       :signature :valid}
      (assoc :aud (if (= (:aud token)
                         client-id)
                    :valid
                    :invalid))
      (assoc :iss (if (= (:iss token)
                         (str "https://cognito-idp."
                              region
                              ".amazonaws.com/"
                              user-pool-id))
                    :valid
                    :invalid))
      (assoc :exp (if (> (:exp token)
                         (util/get-current-time-ms))
                    :valid
                    :invalid))))

(defn fetch-jwks-keys
  [ctx]
  (let [region (util/get-env "Region")
        jwks-json (util/load-config "jwks.json")]
    (log/debug "Initializing JWKS" (get jwks-json :keys))
    (-> ctx
        (assoc :jwks-all (get jwks-json :keys))
        (assoc-in [:env :region] region))))

(defn parse-token
  [{:keys [jwks-all] :as ctx} token]
  (log/debug "Parsing JWT token")
  (try
    (let [jwt (JWT/decode token)
          token-kid (.getKeyId jwt)
          jwks (first
                (filter
                 #(= (:kid %)
                     token-kid)
                 jwks-all))]
      (if jwks
        (let [jwk (Jwk/fromValues (walk/stringify-keys jwks))]

          (try
            (.verify (Algorithm/RSA256
                      (.getPublicKey jwk)
                      nil) jwt)
            (let [resp (validate-token-attributes
                        ctx
                        {:iss (.getIssuer jwt)
                         :aud (.get (.getAudience jwt) 0)
                         :exp (.getTime
                               (.getExpiresAt jwt))})
                  invalid (first
                           (filter
                            (fn [[k v]]
                              (= v :invalid))
                            resp))]
              (if-not invalid
                (assoc ctx
                       :user {:id    (.asString (.getClaim jwt "email"))
                              :email (.asString (.getClaim jwt "email"))
                              :roles (cons
                                      :anonymous
                                      (map
                                       keyword
                                       (vec
                                        (.asArray (.getClaim jwt "cognito:groups")
                                                  String))))})
                (assoc ctx
                       :body {:error resp})))

            (catch SignatureVerificationException e
              (log/error "Unable to verify signature" e)
              (assoc ctx
                     :body {:error {:jwk       :valid
                                    :signature :invalid}}))))
        (assoc ctx
               :body {:error {:jwk :invalid}})))
    (catch Exception e
      (log/error "Unable to parse token" e)
      (assoc ctx
             :body {:error {:jwt :invalid}}))))



