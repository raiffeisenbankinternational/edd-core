(ns lambda.filters
  (:require [lambda.util :as util]
            [clojure.tools.logging :as log]
            [lambda.uuid :as uuid?]
            [aws :as aws]
            [clojure.string :as str]
            [lambda.jwt :as jwt]
            [lambda.uuid :as uuid]))

(def from-queue
  {:cond (fn [{:keys [body]}]
           (if (and
                (contains? body :Records)
                (= (:eventSource (first (:Records body))) "aws:sqs"))
             true
             false))
   :fn (fn [{:keys [body] :as ctx}]
         (assoc ctx
                :body (util/to-edn (-> body
                                       (:Records)
                                       (first)
                                       (:body)))))})

(defn parse-key
  [key]
  (try
    (let [parts (str/split key #"/")
          realm (first parts)
          parts (rest parts)
          realm (if (= realm "upload")
                  "prod"
                  realm)
          parts (if (re-matches #"[\d]{4}-[\d]{2}-[\d]{2}" (first parts))
                  (rest parts)
                  parts)
          interaction-id (first parts)
          parts (rest parts)
          request-id (-> parts
                         (first)
                         (str/split #"\.")
                         (first))]
      {:request-id (uuid/parse request-id)
       :interaction-id (uuid/parse interaction-id)
       :realm realm})
    (catch Exception e
      (log/error "Unable to parse key. Shouls be in format /{{ realm }}/{{ uuid }}/{{ uuid }}.*")
      (throw (ex-info "Unable to parse key"
                      {:key key})))))

(def from-bucket
  {:cond (fn [{:keys [body]}]
           (if (and
                (contains? body :Records)
                (= (:eventSource (first (:Records body))) "aws:s3")) true))
   :fn (fn [{:keys [body] :as ctx}]
         (-> ctx
             (assoc-in [:user :id] (name (:service-name ctx)))
             (assoc-in [:user :role] :non-interactive)
             (assoc :body
                    (let [record (first (:Records body))
                          key (get-in record [:s3 :object :key])
                          bucket (get-in record [:s3 :bucket :name])]
                      (if-not (str/ends-with? key "/")
                        (let [{:keys [request-id
                                      interaction-id
                                      realm]} (parse-key key)]
                          {:request-id request-id
                           :interaction-id interaction-id
                           :user (name
                                  (:service-name ctx))
                           :meta {:realm (keyword realm)
                                  :user {:id request-id
                                         :email "non-interractiva@s3.amazonws.com"
                                         :role :non-interactive}}
                           :commands [{:cmd-id :object-uploaded
                                       :id request-id
                                       :body (aws/get-object record)
                                       :key key}]})
                        {:skip true})))))})

(defn has-role?
  [user role]
  (some #(= role %)
        (get user :roles [])))

(defn get-realm
  [body {:keys [roles]} role]
  (let [realm-prefix "realm-"
        realm (->> roles
                   (map name)
                   (filter #(str/starts-with? % realm-prefix))
                   (first))]
    (when-not realm
      (throw (ex-info (str "Realm: " realm) {:error "Missing realm in request token"})))
    (keyword (subs realm (count realm-prefix)))))

(defn non-interactive
  [user]
  (first
   (filter
    #(= % :non-interactive)
    (:roles user))))

(defn check-user-role
  [{:keys [body req] :as ctx}]
  (let [{:keys [user body]} (jwt/parse-token
                             ctx
                             (or (get-in req [:headers :x-authorization])
                                 (get-in req [:headers :X-Authorization])))
        role (or (get-in body [:user :selected-role])
                 (non-interactive user)
                 (first (remove
                         #(or (= % :anonymous)
                              (str/starts-with? (name %)
                                                "realm-"))
                         (:roles user))))]

    (cond
      (:error body) (assoc ctx :body body)
      (= role :non-interactive) (assoc ctx :meta (:meta body))
      (has-role? user role) (assoc ctx :user {:id (:id user)
                                              :email (:email user)
                                              :role role
                                              :roles (:roles user [])}
                                   :meta {:realm (get-realm body user role)
                                          :user {:id (:id user)
                                                 :email (:email user)
                                                 :role role}})
      :else (assoc ctx :user {:id "anonymous"
                              :email "anonymous"
                              :role :anonymous}))))

(def from-api
  {:init jwt/fetch-jwks-keys
   :cond (fn [{:keys [body]}]
           (contains? body :path))
   :fn (fn [{:keys [req body] :as ctx}]
         (cond
           (= (:path req)
              "/health") (assoc ctx :health-check true)
           (= (:httpMethod req)
              "OPTIONS") (assoc ctx :health-check true)
           :else (-> ctx
                     (assoc :body (util/to-edn (:body body)))
                     (check-user-role))))})

(defn to-api
  [{:keys [resp] :as ctx}]
  (log/debug "to-api" resp)
  (assoc ctx
         :resp {:statusCode 200
                :isBase64Encoded false
                :headers {"Access-Control-Allow-Headers" "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                          "Access-Control-Allow-Methods" "OPTIONS,POST,PUT,GET"
                          "Access-Control-Expose-Headers" "*"
                          "Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                :body (util/to-json resp)}))
