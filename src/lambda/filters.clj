(ns lambda.filters
  (:require [lambda.util :as util]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [sdk.aws.s3 :as s3]
            [lambda.jwt :as jwt]
            [aws.aws :as aws]
            [lambda.uuid :as uuid]))

(def from-queue
  {:cond (fn [{:keys [body]}]
           (if (and
                (contains? body :Records)
                (= (:eventSource (first (:Records body))) "aws:sqs"))
             true
             false))
   :fn   (fn [{:keys [body] :as ctx}]
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
          date (if (re-matches #"[\d]{4}-[\d]{2}-[\d]{2}" (first parts))
                 (first parts)
                 (throw (ex-info "Missing date" {:parts parts
                                                 :error "Missing date"})))
          parts (rest parts)
          interaction-id (first parts)
          parts (rest parts)
          id (if (= 2 (count parts))
               (first parts)
               (-> parts
                   (first)
                   (str/split #"\.")
                   (first)))
          request-id (if (= 2 (count parts))
                       (-> parts
                           (second)
                           (str/split #"\.")
                           (first))
                       id)]
      {:request-id     (uuid/parse request-id)
       :interaction-id (uuid/parse interaction-id)
       :id             (uuid/parse (or id request-id))
       :date           date
       :realm          realm})
    (catch Exception e
      (log/error "Unable to parse key. Should be in format
                  /{{ realm }}/{{ yyyy-MM-dd }}/{{ interaction-id uuid }}/{{ request-id uuid }}.*
                  or
                 /{{ realm }}/{{ yyyy-MM-dd }}/{{ interaction-id uuid }}/{{ request-id uuid }}/{{ id uuid }}.* "
                 e)
      (throw (ex-info "Unable to parse key"
                      {:key  key
                       :data (ex-data e)})))))

(def from-bucket
  {:cond (fn [{:keys [body]}]
           (if (and
                (contains? body :Records)
                (= (:eventSource (first (:Records body))) "aws:s3")) true))
   :fn   (fn [{:keys [body] :as ctx}]
           (-> ctx
               (assoc-in [:user :id] (name (:service-name ctx)))
               (assoc-in [:user :role] :non-interactive)
               (assoc :body
                      (let [record (first (:Records body))
                            key (get-in record [:s3 :object :key])
                            bucket (get-in record [:s3 :bucket :name])]
                        (log/info "Parsing key" key)
                        (if-not (str/ends-with? key "/")
                          (let [{:keys [request-id
                                        interaction-id
                                        date
                                        realm
                                        id] :as parsed-key} (parse-key key)]
                            (log/info "Parsing success " parsed-key)
                            {:request-id     request-id
                             :interaction-id interaction-id
                             :user           (name (:service-name ctx))
                             :meta           {:realm (keyword realm)
                                              :user  {:id    request-id
                                                      :email "non-interractiva@s3.amazonws.com"
                                                      :role  :non-interactive}}
                             :commands       [{:cmd-id :object-uploaded
                                               :id     id
                                               :body   (s3/get-object ctx record)
                                               :date   date
                                               :bucket bucket
                                               :key    key}]})
                          {:skip true})))))})

(defn has-role?
  [user role]
  (some #(= role %)
        (get user :roles [])))

(defn get-realm
  [_ {:keys [roles]} _]
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

(defn extract-m2m-user [authorizer]
  (let [groups (-> authorizer
                   :claims
                   :cognito:groups
                   (str/split #","))
        groups (map keyword groups)]
    {:id    (-> authorizer :claims :email)
     :roles groups
     :role  (first groups)
     :email (-> authorizer :claims :email)}))

(defmulti check-user-role
  (fn [ctx]
    (get-in (:req ctx) [:requestContext :authorizer :claims :token_use]
            (get-in (:req ctx) [:requestContext :authorizer :token_use]))))

(defn parse-authorizer-user
  [ctx]
  (let [user (extract-m2m-user (-> ctx :req :requestContext :authorizer))
        role (or (-> ctx :body :user :selected-role)
                 (non-interactive user)
                 (first (remove #(or (= % :anonymous)
                                     (str/starts-with? (name %) "realm-"))
                                (:roles user))))
        user (assoc user :role role)]
    (assoc ctx :user user
           :meta {:realm (get-realm nil user nil)
                  :user  (assoc user
                                :department-code "000"
                                :department "No Department")})))

(defmethod check-user-role "id" [ctx]
  (parse-authorizer-user ctx))

(defmethod check-user-role "m2m" [ctx]
  (let [ctx (assoc-in ctx [:req :requestContext :authorizer :claims]
                      (get-in ctx [:req :requestContext :authorizer]))]
    (parse-authorizer-user ctx)))

(defmethod check-user-role :default
  [{:keys [_ req] :as ctx}]
  (let [{:keys [user body]} (jwt/parse-token
                             ctx
                             (or (get-in req [:headers :x-authorization])
                                 (get-in req [:headers :X-Authorization])))
        role (or (-> body :user :selected-role)
                 (non-interactive user)
                 (first (remove
                         #(or (= % :anonymous)
                              (str/starts-with? (name %)
                                                "realm-"))
                         (:roles user))))]

    (cond
      (:error body) (assoc ctx :body body)
      (= role :non-interactive) (assoc ctx :meta (:meta body))
      (has-role? user role) (assoc ctx :user {:id    (:id user)
                                              :email (:email user)
                                              :role  role
                                              :roles (:roles user [])}
                                   :meta {:realm (get-realm body user role)
                                          :user  {:id              (:id user)
                                                  :email           (:email user)
                                                  :role            role
                                                  :department-code (:department-code user "000")
                                                  :department      (:department user "No Department")}})
      :else (assoc ctx :user {:id    "anonymous"
                              :email "anonymous"
                              :role  :anonymous}))))

(def from-api
  {:init jwt/fetch-jwks-keys
   :cond (fn [{:keys [body]}]
           (contains? body :path))
   :fn   (fn [{:keys [req body] :as ctx}]
           (let [{http-method :httpMethod
                  path        :path} req]
             (cond
               (= path "/health")
               (assoc ctx :health-check true)

               (= http-method "OPTIONS")
               (assoc ctx :health-check true)

               :else (-> ctx
                         (assoc :body (util/to-edn (:body body)))
                         (check-user-role)))))})

(defn to-api
  [{:keys [resp resp-content-type resp-serializer-fn]
    :or   {resp-content-type  "application/json"
           resp-serializer-fn util/to-json}
    :as   ctx}]
  (log/debug "to-api" resp)
  (let [resp (aws/produce-compatible-error-response resp)]
    (assoc ctx
           :resp {:statusCode      200
                  :isBase64Encoded false
                  :headers         {"Access-Control-Allow-Headers"  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                                    "Access-Control-Allow-Methods"  "OPTIONS,POST,PUT,GET"
                                    "Access-Control-Expose-Headers" "*"
                                    "Content-Type"                  resp-content-type
                                    "Access-Control-Allow-Origin"   "*"}
                  :body            (resp-serializer-fn resp)})))
