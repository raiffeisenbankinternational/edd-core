(ns sdk.aws.s3
  (:require [sdk.aws.common :as common]
            [lambda.util :as util]
            [lambda.http-client :as client]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn get-host
  [ctx]
  (str "s3."
       (get-in ctx [:aws :region])
       ".amazonaws.com"))

(defn convert-query-params
  [params]
  (reduce
   (fn [p [k v]]
     (assoc p k v))
   {}
   params))

(defn- parse-response
  [response object]
  (log/debug "Auth response" response)
  (cond
    (contains? response :error) (do
                                  (log/error "Failed update" response)
                                  {:error (:error response)})
    (> (:status response 199) 299) (do
                                     (log/error "S3 Response failed"
                                                (:status response)
                                                (:body response))
                                     {:error {:status  (:status response)
                                              :message (slurp (:body response))
                                              :key     (get-in object [:s3 :object :key])
                                              :bucket  (get-in object [:s3 :bucket :name])}})
    :else response))

(defn get-aws-token [{:keys [aws]}]
  (let [token (:aws-session-token aws)]
    (if (empty? token)
      (System/getenv "AWS_SESSION_TOKEN")
      token)))

(defn s3-request-helper [{:keys [aws] :as ctx} object]
  {:headers    (merge {"Host"                 (get-host ctx)
                       "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                       "x-amz-date"           (common/create-date)
                       "x-amz-security-token" (get-aws-token ctx)}
                      (get-in object [:s3 :headers]))
   :service    "s3"
   :region     (:region aws)
   :access-key (:aws-access-key-id aws)
   :secret-key (:aws-secret-access-key aws)})

(defn put-object
  "puts object.content (should be plain string) into object.s3.bucket.name under object.s3.bucket.key"
  [{:keys [aws] :as ctx} object]
  (let [req

        (merge (s3-request-helper ctx object)
               {:method     "PUT"
                :uri        (str "/"
                                 (get-in object [:s3 :bucket :name])
                                 "/"
                                 (get-in object [:s3 :object :key]))})
        common (common/authorize req)
        response (client/retry-n #(-> (util/http-put
                                       (str "https://"
                                            (get (:headers req) "Host")
                                            (:uri req))
                                       (client/request->with-timeouts
                                        %
                                        {:as      :stream
                                         :headers (-> (:headers req)
                                                      (dissoc "Host")
                                                      (assoc "Authorization" common))
                                         :body    (get-in object [:s3 :object :content])})
                                       :raw true)
                                      (parse-response object)))
        {:keys [error] :as response} response]
    (if error
      response
      (io/reader (:body response) :encoding "UTF-8"))))

(defn get-object
  [{:keys [aws] :as ctx} object]
  (let [req
        (merge (s3-request-helper ctx object)
               {:method     "GET"
                :uri        (str "/"
                                 (get-in object [:s3 :bucket :name])
                                 "/"
                                 (get-in object [:s3 :object :key]))})
        common (common/authorize req)
        response (client/retry-n #(-> (util/http-get
                                       (str "https://"
                                            (get (:headers req) "Host")
                                            (:uri req))
                                       (client/request->with-timeouts
                                        %
                                        {:as      :stream
                                         :headers (-> (:headers req)
                                                      (dissoc "Host")
                                                      (assoc "Authorization" common))})
                                       :raw true)
                                      (parse-response  object)))
        {:keys [error] :as response} response]
    (if error
      response
      (io/reader (:body response) :encoding "UTF-8"))))

(defn delete-object
  [{:keys [aws] :as ctx} object]
  (let [req
        (merge (s3-request-helper ctx object)
               {:method     "DELETE"
                :uri        (str "/"
                                 (get-in object [:s3 :bucket :name])
                                 "/"
                                 (get-in object [:s3 :object :key]))})

        common (common/authorize req)
        response (client/retry-n #(-> (util/http-delete
                                       (str "https://"
                                            (get (:headers req) "Host")
                                            (:uri req))
                                       (client/request->with-timeouts
                                        %
                                        {:as      :stream
                                         :headers (-> (:headers req)
                                                      (dissoc "host")
                                                      (assoc "Authorization" common))})
                                       :raw true)
                                      (parse-response object)))]
    (if (:error response)
      response
      {:body    (io/reader (:body response) :encoding "UTF-8")
       :headers (:headers response)})))

(defn get-object-tagging
  [{:keys [aws] :as ctx} object]
  (let [req {:method     "GET"
             :uri        (str "/" (get-in object [:s3 :object :key]))
             :query      [["tagging" "True"]]
             :headers    {"Host"                 (str
                                                  (get-in object [:s3 :bucket :name])
                                                  ".s3."
                                                  (:region aws)
                                                  ".amazonaws.com")
                          "Content-Type"         "application/json"
                          "Accept"               "application/json"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (get-aws-token ctx)}
             :service    "s3"
             :region     (:region aws)
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        common (common/authorize req)]

    (let [response (client/retry-n #(-> (util/http-get
                                         (str "https://"
                                              (get (:headers req) "Host")
                                              (:uri req))
                                         (client/request->with-timeouts
                                          %
                                          {:as      :stream
                                           :query-params (convert-query-params (:query req))
                                           :headers      (-> (:headers req)
                                                             (dissoc "host")
                                                             (assoc "Authorization" common))})
                                         :raw true)
                                        (parse-response object)))]
      (if (:error response)
        response
        (->> (xml/parse (:body response))
             (:content)
             (first)
             (:content)
             (mapv
              (fn [{:keys [content]}]
                (let [key (first content)
                      val (second content)]
                  (assoc
                   {}
                   (-> key
                       (:tag)
                       (name)
                       (str/lower-case)
                       (keyword))
                   (-> key
                       (:content)
                       (first))
                   (-> val
                       (:tag)
                       (name)
                       (str/lower-case)
                       (keyword))
                   (-> val
                       (:content)
                       (first)))))))))))

(defn put-object-tagging
  [{:keys [aws] :as ctx} {:keys [object tags]}]
  (let [tags (xml/emit-str
              {:tag     "Tagging"
               :content [{:tag     "TagSet"
                          :content (mapv
                                    (fn [{:keys [key value]}]
                                      {:tag     "Tag"
                                       :content [{:tag     "Key"
                                                  :content [key]}
                                                 {:tag     "Value"
                                                  :content [value]}]})
                                    tags)}]})
        req {:method     "PUT"
             :uri        (str "/" (get-in object [:s3 :object :key]))
             :query      [["tagging" "True"]]
             :headers    {"Host"                 (str
                                                  (get-in object [:s3 :bucket :name])
                                                  ".s3."
                                                  (:region aws)
                                                  ".amazonaws.com")
                          "Content-Type"         "application/json"
                          "Accept"               "application/json"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (get-aws-token ctx)}
             :service    "s3"
             :region     (:region aws)
             :payload    tags
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        common (common/authorize req)]
    (let [response (client/retry-n #(-> (util/http-put
                                         (str "https://"
                                              (get (:headers req) "Host")
                                              (:uri req))
                                         (client/request->with-timeouts
                                          %
                                          {:as      :stream
                                           :query-params (convert-query-params (:query req))
                                           :body         (:payload req)
                                           :headers      (-> (:headers req)
                                                             (dissoc "host")
                                                             (assoc "Authorization" common))})
                                         :raw true)
                                        (parse-response object)))]
      (if (:error response)
        response
        {:version (get-in response [:headers :x-amz-version-id])}))))
