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
   :fn   (fn [{:keys [body] :as ctx}]
           (assoc ctx
                  :body (util/to-edn (-> body
                                         (:Records)
                                         (first)
                                         (:body)))))})

(def from-bucket
  {:cond (fn [{:keys [body]}]
           (if (and
                (contains? body :Records)
                (= (:eventSource (first (:Records body))) "aws:s3")) true))
   :fn   (fn [{:keys [body] :as ctx}]
           (-> ctx
               (assoc-in [:user :id] (name (:service-name ctx)))
               (assoc-in [:user :role] :non-interactive)
               (assoc :body {:request-id     (get ctx :request-id (uuid/gen))
                             :interaction-id (get ctx :interaction-id (uuid/gen))
                             :user           (name
                                              (:service-name ctx))
                             :role           :non-interactive
                             :commands       (into []
                                                   (for [record (:Records body)]
                                                     {:cmd-id :object-uploaded
                                                      :id     (get ctx :request-id (uuid/gen))
                                                      :body   (aws/get-object record)
                                                      :key    (get-in record [:s3 :object :key])}))})))})

(defn has-role?
  [user role]
  (some #(= role %)
        (get user :roles [])))

(defn check-user-role
  [{:keys [body req] :as ctx}]
  (let [{:keys [user body]} (jwt/parse-token
                             ctx
                             (or (get-in req [:headers :x-authorization])
                                 (get-in req [:headers :X-Authorization])))
        role (get-in body [:user :selected-role])]
    (cond
      (:error body) (assoc ctx :body body)
      (has-role? user role) (assoc ctx :user {:id    (:id user)
                                              :email (:email user)
                                              :role  role})
      :else (assoc ctx :user {:id    "anonymous"
                              :email "anonymous"
                              :role  :anonymous}))))

(def from-api
  {:init jwt/fetch-jwks-keys
   :cond (fn [{:keys [body]}]
           (contains? body :path))
   :fn   (fn [{:keys [req body] :as ctx}]
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
         :resp {:statusCode      200
                :isBase64Encoded false
                :headers         {"Access-Control-Allow-Headers"  "Id, VersionId, X-Authorization,Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                                  "Access-Control-Allow-Methods"  "OPTIONS,POST,PUT,GET"
                                  "Access-Control-Expose-Headers" "*"
                                  "Content-Type"                  "application/json"
                                  "Access-Control-Allow-Origin"   "*"}
                :body            (util/to-json resp)}))
